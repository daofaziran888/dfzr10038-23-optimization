"""回溯批次容量计量交付的去重与对账验证。

核心场景：一笔回溯在存储写入后断开连接（生产侧没收到回执），
并发重发两次；另一笔实际完成的回溯单独交付；最后逐笔对账。
"""
from __future__ import annotations

import concurrent.futures

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.capacity_ledger import capacity_ledger, parse_row_count
from app.store import store

client = TestClient(app)


@pytest.fixture(autouse=True)
def fresh_state():
    store.reset()
    capacity_ledger.reset()
    yield


def _make_backfill(entry_id: int, job_no: str, rows_text, status: str = "已完成") -> dict:
    row = {
        "id": entry_id, "status": status, "pending": False, "abnormal": False,
        "任务编号": job_no, "目标特征": f"特征-{job_no}", "起始日期": "2026-09-01",
        "截止日期": "2026-09-02", "回溯模式": "全量", "并行度": "4",
        "当前进度": "100%", "数据行数": rows_text,
    }
    store.rows("backfill_job").append(row)
    return row


def _set_store_configs(*config_nos: str) -> None:
    store._tables["feature_store"] = [
        {
            "id": index + 1, "status": "草稿", "pending": True, "abnormal": False,
            "配置编号": config_no, "特征列表": "特征A", "存储引擎": "Hive",
            "分区键": "dt", "TTL设置": "30d", "压缩策略": "gzip",
            "存储路径": "/warehouse/features", "存储体积": "0GB",
        }
        for index, config_no in enumerate(config_nos)
    ]


def _draft_configs() -> list[dict]:
    return [row for row in store.rows("feature_store") if row.get("status") == "草稿"]


# ---------------- 行数口径兼容 ----------------

def test_parse_row_count_legacy_formats():
    assert parse_row_count("890000行") == 890000      # 历史回溯的 "N行" 口径
    assert parse_row_count("1250000") == 1250000      # 纯数字字符串口径
    assert parse_row_count("1,250,000 行") == 1250000  # 千分位口径
    assert parse_row_count(890000) == 890000          # 数值口径
    assert parse_row_count("特征回溯样例1") is None     # 未确认的样例文本
    assert parse_row_count("") is None
    assert parse_row_count(None) is None


# ---------------- 断连重发去重 ----------------

def test_redeliver_after_disconnect_dedupes_concurrently():
    """存储写入后断开连接，并发重发两次：容量只记一次。"""
    _set_store_configs("FEAT-A", "FEAT-B")
    backfill = _make_backfill(101, "BACK-0101", "360000行")

    # 第一次送达：存储已写入并保存回执，但连接断开，生产侧视为未收到
    first = capacity_ledger.deliver_backfill(backfill)
    assert first["ok"] and not first["duplicate"]
    lost_receipt_no = first["receipt"]["回执编号"]  # 生产侧没拿到这张回执

    # 生产侧没收到回执，并发重发两次
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        retried = list(pool.map(lambda _: capacity_ledger.deliver_backfill(backfill), range(2)))

    # 两次重发都命中同一张已保存的回执，按重复送达去重
    assert all(r["ok"] and r["duplicate"] for r in retried)
    assert {r["receipt"]["回执编号"] for r in retried} == {lost_receipt_no}

    # 只认领了一份草稿配置，另一份保持草稿，不会被重复认领
    drafts = _draft_configs()
    assert len(drafts) == 1 and drafts[0]["配置编号"] == "FEAT-B"
    claimed = store.rows("feature_store")[0]
    assert claimed["配置编号"] == "FEAT-A"
    assert claimed["status"] == "已启用"
    # 同一份行数只累加一次
    assert claimed["storage_size_gb"] == round(360000 * 0.0001, 2)

    # 台账里该批次只有一条回执、一份计量
    receipts = capacity_ledger.receipts()
    assert len(receipts) == 1
    assert receipts[0]["产出来源"] == "backfill_job/BACK-0101"
    assert receipts[0]["配置编号"] == "FEAT-A"
    assert receipts[0]["行数"] == 360000


def test_second_backfill_accounted_separately_and_reconciled():
    """另一笔实际完成的回溯单独核算，两笔与容量汇总逐笔对得上。"""
    _set_store_configs("FEAT-A", "FEAT-B")
    backfill_a = _make_backfill(101, "BACK-0101", "360000行")
    backfill_b = _make_backfill(102, "BACK-0102", "1250000")  # 历史纯数字口径

    # 批次A：写入后断连，再重发两次（串行重发同样去重）
    capacity_ledger.deliver_backfill(backfill_a)
    capacity_ledger.deliver_backfill(backfill_a)
    capacity_ledger.deliver_backfill(backfill_a)
    # 批次B：另一笔实际完成的回溯，单独交付一次
    second = capacity_ledger.deliver_backfill(backfill_b)
    assert second["ok"] and not second["duplicate"]

    # 两笔各占一份配置、各有一张回执
    receipts = {r["产出来源"]: r for r in capacity_ledger.receipts()}
    assert set(receipts) == {"backfill_job/BACK-0101", "backfill_job/BACK-0102"}
    assert receipts["backfill_job/BACK-0101"]["行数"] == 360000
    assert receipts["backfill_job/BACK-0102"]["行数"] == 1250000
    assert receipts["backfill_job/BACK-0101"]["配置编号"] != receipts["backfill_job/BACK-0102"]["配置编号"]

    # 逐笔对账：回溯清单与容量汇总一一对应、两侧汇总相等
    result = capacity_ledger.reconcile()
    assert result["summary"]["逐笔相符"] is True
    assert result["summary"]["回溯批次笔数"] == 2
    assert result["summary"]["回执笔数"] == 2
    assert result["summary"]["回溯确认行数汇总"] == 360000 + 1250000
    assert result["summary"]["容量汇总行数"] == 360000 + 1250000
    lines = {line["任务编号"]: line for line in result["lines"]}
    assert lines["BACK-0101"]["已入账"] and lines["BACK-0101"]["差异"] == 0
    assert lines["BACK-0102"]["已入账"] and lines["BACK-0102"]["差异"] == 0
    assert lines["BACK-0101"]["配置编号"] == receipts["backfill_job/BACK-0101"]["配置编号"]


# ---------------- 计量确认后沿用 ----------------

def test_retry_reuses_confirmed_source_and_rows():
    """行数与目标特征一经确认便能回查，重试沿用同一来源和行数。"""
    _set_store_configs("FEAT-A")
    backfill = _make_backfill(103, "BACK-0103", "待统计")  # 尚未确认行数

    first = capacity_ledger.deliver_backfill(backfill)
    confirmed_rows = first["receipt"]["行数"]
    assert confirmed_rows > 0
    # 确认后写回任务，可回查
    assert backfill["数据行数"] == f"{confirmed_rows}行"
    metering = capacity_ledger.metering_for("BACK-0103")
    assert metering["行数"] == confirmed_rows
    assert metering["目标特征"] == "特征-BACK-0103"

    # 重试沿用同一来源和行数，不重新生成
    retry = capacity_ledger.deliver_backfill(backfill)
    assert retry["duplicate"] and retry["receipt"]["行数"] == confirmed_rows


def test_no_draft_config_defers_receipt_until_resend():
    """无可用草稿配置时不入账、不出回执；补配后重送沿用已确认计量。"""
    _set_store_configs()  # 没有草稿配置
    backfill = _make_backfill(104, "BACK-0104", "720000行")

    deferred = capacity_ledger.deliver_backfill(backfill)
    assert not deferred["ok"] and deferred["receipt"] is None
    assert capacity_ledger.receipts() == []

    # 值班员补配草稿配置后接着送，沿用同一行数
    _set_store_configs("FEAT-C")
    resent = capacity_ledger.deliver_backfill(backfill)
    assert resent["ok"] and not resent["duplicate"]
    assert resent["receipt"]["行数"] == 720000
    assert resent["receipt"]["配置编号"] == "FEAT-C"


# ---------------- HTTP 层：动作交付 + 工作台对账 ----------------

def test_api_deliver_action_dedupes_and_reconciles():
    """走接口：回溯批次断连后并发重发两次，与另一笔回溯逐笔对账。"""
    _set_store_configs("FEAT-A", "FEAT-B")
    _make_backfill(101, "BACK-0101", "360000行")
    _make_backfill(102, "BACK-0102", "890000行")

    def deliver(entry_id: int):
        return client.post(
            f"/api/backfill_job/{entry_id}/actions", json={"action": "交付容量计量"})

    # 批次A：首次送达后连接断开（忽略响应），并发重发两次
    first = deliver(101)
    assert first.status_code == 200 and first.json()["ok"]
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        retried = list(pool.map(lambda _: deliver(101), range(2)))
    assert all(r.status_code == 200 and r.json()["ok"] for r in retried)
    assert all("已入账" in r.json()["message"] for r in retried)

    # 批次B：另一笔实际完成的回溯，单独核算
    second = deliver(102)
    assert second.status_code == 200 and second.json()["ok"]

    # 回执逐笔可查：配置编号、产出来源、行数齐全
    receipts = client.get("/api/feature_store/receipts").json()
    assert receipts["total"] == 2
    by_source = {item["产出来源"]: item for item in receipts["items"]}
    assert by_source["backfill_job/BACK-0101"]["行数"] == 360000
    assert by_source["backfill_job/BACK-0102"]["行数"] == 890000
    assert by_source["backfill_job/BACK-0101"]["配置编号"] == "FEAT-A"
    assert by_source["backfill_job/BACK-0102"]["配置编号"] == "FEAT-B"

    # 存储工作台刷新时的对账视图：回溯清单与容量汇总逐笔对得上
    reconcile = client.get("/api/feature_store/reconcile").json()
    assert reconcile["ok"] is True
    assert reconcile["summary"]["容量汇总行数"] == 360000 + 890000
    assert reconcile["summary"]["逐笔相符"] is True
    assert all(line["已入账"] and line["差异"] == 0 for line in reconcile["lines"])

    # 批次A只认领了 FEAT-A，FEAT-B 留给批次B，没有第三份配置被误认领
    stores = client.get("/api/feature_store").json()["items"]
    assert sum(1 for row in stores if row["status"] == "已启用") == 2


def test_api_deliver_rejects_unfinished_backfill():
    _set_store_configs("FEAT-A")
    _make_backfill(105, "BACK-0105", "1000行", status="回溯中")
    response = client.post("/api/backfill_job/105/actions", json={"action": "交付容量计量"})
    assert response.status_code == 200
    assert response.json()["ok"] is False
    assert capacity_ledger.receipts() == []
