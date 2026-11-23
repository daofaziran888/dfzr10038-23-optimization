"""容量计量专用接口：接收回执查询与回溯-容量逐笔对账。

固定路径必须早于通用资源路由 /api/{module}/{entry_id} 注册，
否则 /api/feature_store/reconcile 会被当成 entry_id 匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from app.services.capacity_ledger import capacity_ledger

router = APIRouter(prefix="/api/feature_store", tags=["容量计量"])


@router.get("/receipts")
def list_receipts() -> dict[str, Any]:
    """接收回执列表：特征存储确认容量入账的逐笔凭证。"""
    receipts = capacity_ledger.receipts()
    return {"items": receipts, "total": len(receipts)}


@router.get("/reconcile")
def reconcile_capacity() -> dict[str, Any]:
    """回溯清单与容量汇总逐笔对账，供存储工作台刷新时核对。"""
    result = capacity_ledger.reconcile()
    return {"ok": result["summary"]["逐笔相符"], **result}
