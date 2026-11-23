"""回溯任务领域服务：字段、状态和动作来自业务配置，容量计量交付走台账幂等去重。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.capacity_ledger import capacity_ledger
from app.store import store


class BackfillJobService(BaseModuleService):
    MODULE = "backfill_job"
    ENTITY = "回溯任务"
    OPERATION_TERM = "计算"
    LIST_FIELDS = ["任务编号", "目标特征", "起始日期", "截止日期", "回溯模式", "并行度", "当前进度", "数据行数"]
    REQUIRED_FIELDS = ["任务编号", "目标特征", "起始日期"]
    STATUS_ORDER = ["待执行", "回溯中", "已完成", "部分缺失"]
    ACTION_RULES = {"启动回溯": "回溯中", "暂停回溯": "已完成", "标记缺失": "部分缺失", "交付容量计量": "已完成"}
    NEGATIVE_ACTIONS = []

    DELIVER_ACTION = "交付容量计量"

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action != self.DELIVER_ACTION:
            return super().run_action(entry_id, action)
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if entry.get("status") != "已完成":
            return None, f"回溯任务 {entry.get('任务编号', '')} 尚未完成，不能交付容量计量"
        # 交付是幂等的：重复送达只记一次容量，返回台账中已保存的回执
        result = capacity_ledger.deliver_backfill(entry)
        if not result["ok"]:
            return None, str(result["message"])
        return entry, str(result["message"])
