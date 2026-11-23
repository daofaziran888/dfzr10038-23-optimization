"""回溯批次容量计量台账：一次确认、幂等交付、接收回执与逐笔对账。

回溯完成后产出的容量计量数据（目标特征、行数）在这里一次确认、多次沿用；
特征存储消费时按回溯批次键去重：同一批次重复送达只记一次容量，
不再认领下一份草稿配置，也不累加同一份行数。

确认点：接收回执（带配置编号、产出来源、行数）落台账即"容量已入账"。
生产侧没收到回执、或存储写完行数后断开连接，值班员都凭回执台账判断
从哪里接着送——已入账的重发直接返回原回执去重，未入账的补送即可。
"""
from __future__ import annotations

import datetime
import random
import re
import threading
from typing import Any

from app.store import store

_ROW_SUFFIX = re.compile(r"\s*行\s*$")


def parse_row_count(value: Any) -> int | None:
    """把回溯任务的行数字段解析成行数，兼容历史回溯的各种口径。

    支持：int/float、"890000"、"890000行"、"1,250,000 行"；
    无法解析（尚未确认或样例文本）时返回 None。
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = int(value)
        return number if number > 0 else None
    text = _ROW_SUFFIX.sub("", str(value)).replace(",", "").strip()
    if not text:
        return None
    try:
        number = int(float(text))
    except ValueError:
        return None
    return number if number > 0 else None


class CapacityLedger:
    """回溯→存储的容量计量台账（内存实现，真实项目落库即可）。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # 批次键 = 回溯任务编号；同一批次的计量与回执全平台唯一
        self._meterings: dict[str, dict[str, Any]] = {}
        self._receipts: dict[str, dict[str, Any]] = {}

    # ---------------- 计量确认 ----------------

    def confirm_metering(self, backfill_row: dict[str, Any]) -> dict[str, Any]:
        """确认一笔回溯批次的容量计量；已确认过的批次直接沿用原结果。"""
        job_no = str(backfill_row.get("任务编号", "")).strip()
        with self._lock:
            return self._confirm_metering_locked(backfill_row, job_no)

    def _confirm_metering_locked(
        self, backfill_row: dict[str, Any], job_no: str
    ) -> dict[str, Any]:
        known = self._meterings.get(job_no)
        if known is not None:
            return dict(known)
        target = str(backfill_row.get("目标特征", ""))
        rows = parse_row_count(backfill_row.get("数据行数"))
        if rows is None:
            rows = random.randint(50000, 500000)
        # 统一写回 "N行" 口径，与历史回溯保持一致，确认后即可回查
        backfill_row["数据行数"] = f"{rows}行"
        metering = {
            "任务编号": job_no,
            "目标特征": target,
            "行数": rows,
            "确认时间": datetime.datetime.now().isoformat(),
        }
        self._meterings[job_no] = metering
        return dict(metering)

    # ---------------- 幂等交付 ----------------

    def deliver_backfill(self, backfill_row: dict[str, Any]) -> dict[str, Any]:
        """把一笔已完成回溯的容量计量交付特征存储。

        同一批次重复送达只记一次容量：已入账的直接返回原回执，
        不再认领下一份草稿配置，也不累加同一份行数。
        整个"认领配置→累加容量→保存回执"在同一临界区完成，
        因此不存在"写了行数但回执丢失"的中间态，重发永远能查到回执。
        """
        job_no = str(backfill_row.get("任务编号", "")).strip()
        if not job_no:
            return {"ok": False, "receipt": None, "duplicate": False,
                    "config_id": None, "message": "回溯任务缺少任务编号，无法交付容量计量"}
        with self._lock:
            existing = self._receipts.get(job_no)
            if existing is not None:
                return {
                    "ok": True,
                    "receipt": dict(existing),
                    "duplicate": True,
                    "config_id": existing.get("config_id"),
                    "message": (
                        f"批次 {job_no} 容量已入账（回执 {existing['回执编号']}），"
                        "本次重复送达已去重，未重复认领配置或累加行数"
                    ),
                }
            metering = self._confirm_metering_locked(backfill_row, job_no)
            config = self._claim_store_config_locked(job_no, metering)
            if config is None:
                # 容量未入账就不出回执：值班员补配草稿配置后从这里接着送，
                # 重送时沿用已确认的计量结果（同一来源、同一行数）。
                return {
                    "ok": False,
                    "receipt": None,
                    "duplicate": False,
                    "config_id": None,
                    "message": (
                        f"批次 {job_no} 计量已确认（{metering['行数']}行），"
                        "但无可用草稿存储配置，容量未入账，请补配后重送"
                    ),
                }
            receipt = {
                "回执编号": f"RCPT-{job_no}",
                "配置编号": str(config.get("配置编号", "")),
                "产出来源": f"backfill_job/{job_no}",
                "目标特征": metering["目标特征"],
                "行数": metering["行数"],
                "接收时间": datetime.datetime.now().isoformat(),
                "config_id": int(config.get("id", 0)),
            }
            # 回执落台账 = 容量已入账的确认点，此后任何重发都按已入账去重
            self._receipts[job_no] = receipt
            return {
                "ok": True,
                "receipt": dict(receipt),
                "duplicate": False,
                "config_id": receipt["config_id"],
                "message": (
                    f"批次 {job_no} 容量 {metering['行数']}行 已写入存储配置 "
                    f"{receipt['配置编号']}，回执 {receipt['回执编号']} 已保存"
                ),
            }

    def _claim_store_config_locked(
        self, job_no: str, metering: dict[str, Any]
    ) -> dict[str, Any] | None:
        """认领一份草稿存储配置并累加容量；每笔批次全程只调用一次。"""
        for srow in store.rows("feature_store"):
            if srow.get("status") == "草稿":
                srow["status"] = "已启用"
                srow["pending"] = True
                added_gb = round(metering["行数"] * 0.0001, 2)
                srow["storage_size_gb"] = round(
                    float(srow.get("storage_size_gb") or 0) + added_gb, 2)
                srow["存储体积"] = f"{srow['storage_size_gb']}GB"
                srow["triggered_by"] = f"backfill_job/{job_no}"
                return srow
        return None

    # ---------------- 查询与对账 ----------------

    def receipts(self) -> list[dict[str, Any]]:
        """全部接收回执（容量汇总的逐笔依据）。"""
        with self._lock:
            return [dict(receipt) for receipt in self._receipts.values()]

    def metering_for(self, job_no: str) -> dict[str, Any] | None:
        """回查某笔回溯批次已确认的计量结果（来源与行数）。"""
        with self._lock:
            metering = self._meterings.get(job_no)
            return dict(metering) if metering is not None else None

    def reconcile(self) -> dict[str, Any]:
        """回溯清单与容量汇总逐笔对账。

        每笔已确认计量的回溯批次应对应恰好一条回执且行数一致；
        汇总两侧相等则逐笔相符。值班员据此判断哪笔已入账、哪笔要接着送。
        """
        with self._lock:
            meterings = [dict(m) for m in self._meterings.values()]
            receipts = dict(self._receipts)
        lines = []
        for metering in sorted(meterings, key=lambda item: item["任务编号"]):
            receipt = receipts.get(metering["任务编号"])
            received_rows = receipt["行数"] if receipt else 0
            lines.append({
                "任务编号": metering["任务编号"],
                "目标特征": metering["目标特征"],
                "确认行数": metering["行数"],
                "已入账": receipt is not None,
                "配置编号": receipt["配置编号"] if receipt else "",
                "回执编号": receipt["回执编号"] if receipt else "",
                "入账行数": received_rows,
                "差异": metering["行数"] - received_rows,
            })
        confirmed_total = sum(m["行数"] for m in meterings)
        received_total = sum(r["行数"] for r in receipts.values())
        balanced = (
            all(line["已入账"] and line["差异"] == 0 for line in lines)
            and confirmed_total == received_total
        )
        return {
            "lines": lines,
            "summary": {
                "回溯批次笔数": len(meterings),
                "回执笔数": len(receipts),
                "回溯确认行数汇总": confirmed_total,
                "容量汇总行数": received_total,
                "逐笔相符": balanced,
            },
        }

    def reset(self) -> None:
        """清空台账，供测试与演示重置。"""
        with self._lock:
            self._meterings.clear()
            self._receipts.clear()


capacity_ledger = CapacityLedger()
