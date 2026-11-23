"""历史分区导出投影核对与结算复核流程测试。

覆盖空投影判断修复：为历史分区选择不存在的导出字段时，不再返回一串
空对象冒充已完成；投影核对明细按分区实际列定义输出。联合结算导出
（两个日期同属一次结算核对）的复核、交付分区交叉校验、补采待办办结
与结算入账门禁。
"""
from __future__ import annotations

import pytest

from app.fusion_engine import FusionEngine


@pytest.fixture
def engine() -> FusionEngine:
    return FusionEngine()


def _deliver_settlement_partitions(engine: FusionEngine):
    """两个日期同属一次结算核对：旧分区只有 gross，新分区有 amount 但零行。"""
    old_snap = engine.create_snapshot("WT_SETTLE", partition_key_value="20261121")
    engine.deliver_partition_data(
        old_snap.partition_id, ["id", "gross"], [{"id": "k1", "gross": 17}],
    )
    new_snap = engine.create_snapshot("WT_SETTLE", partition_key_value="20261122")
    engine.deliver_partition_data(new_snap.partition_id, ["id", "amount"], [])
    plan = engine.create_export_plan(
        "WT_SETTLE", export_format="parquet", field_filter=["amount"],
    )
    return old_snap, new_snap, plan


def _reference_cv_plan(engine: FusionEngine, ref_amount: float = 15):
    """参照宽表：同键金额，用于交付分区交叉校验。"""
    ref_job = engine.create_fusion_job("WT_SETTLE_REF", ["SRC-REF"], join_key="id")
    ref_job.merged_records = [{"id": "k1", "amount": ref_amount}]
    return engine.create_cross_validate_plan(
        "WT_SETTLE", "WT_SETTLE_REF", join_key="id", validate_fields=["amount"],
    )


class TestEmptyProjectionJudgment:
    """空投影判断：按分区实际列定义输出投影核对明细。"""

    def test_missing_field_records_field_missing_not_empty_objects(
        self, engine: FusionEngine,
    ) -> None:
        """旧分区只有 gross：选 amount 记字段缺失，不再返回空对象。"""
        old_snap, _, plan = _deliver_settlement_partitions(engine)
        result = engine.execute_export_from_snapshot(plan.plan_id, old_snap.partition_id)
        assert result["ok"] is True
        # 修复前：每条记录投影出一个空对象；修复后：字段缺失，不出空对象
        assert result["records"] == []
        assert result["exported_rows"] == 0
        projection = result["projection"]
        assert projection["status"] == "字段缺失"
        assert projection["missing_fields"] == ["amount"]
        assert projection["hit_fields"] == []
        # 按分区实际列定义输出
        assert projection["actual_columns"] == ["id", "gross"]
        assert projection["partition_range"] == "20261121"
        # 复核详情不再把字段缺失显示成已完成
        assert plan.export_status == "字段缺失"

    def test_partial_hit_still_records_missing(self, engine: FusionEngine) -> None:
        """部分命中：命中列正常投影，未命中字段仍记字段缺失。"""
        old_snap, _, _ = _deliver_settlement_partitions(engine)
        plan = engine.create_export_plan("WT_SETTLE", field_filter=["id", "amount"])
        result = engine.execute_export_from_snapshot(plan.plan_id, old_snap.partition_id)
        projection = result["projection"]
        assert projection["hit_fields"] == ["id"]
        assert projection["missing_fields"] == ["amount"]
        assert projection["status"] == "字段缺失"
        assert result["records"] == [{"id": "k1"}]

    def test_valid_empty_detail_keeps_real_header_and_range(
        self, engine: FusionEngine,
    ) -> None:
        """新分区有 amount 但零行：有效空明细带真实列名和分区范围。"""
        _, new_snap, plan = _deliver_settlement_partitions(engine)
        result = engine.execute_export_from_snapshot(plan.plan_id, new_snap.partition_id)
        projection = result["projection"]
        assert projection["status"] == "零行"
        assert projection["header"] == ["amount"]  # 空明细表头保留 amount
        assert projection["actual_columns"] == ["id", "amount"]
        assert projection["partition_range"] == "20261122"
        assert result["exported_rows"] == 0
        assert plan.export_status == "已完成"

    def test_single_partition_export_does_not_touch_other_partition(
        self, engine: FusionEngine,
    ) -> None:
        """只导出新分区时，旧分区没有被当成已出空明细。"""
        _, new_snap, plan = _deliver_settlement_partitions(engine)
        engine.execute_export_from_snapshot(plan.plan_id, new_snap.partition_id)
        audits = engine.projection_audits()
        assert len(audits) == 1
        assert audits[0].partition_range == "20261122"
        assert audits[0].status == "零行"


class TestJointSettlementExport:
    """联合导出：先分别请求两个分区，再合起来出数并立即复核。"""

    def test_joint_export_review_details(self, engine: FusionEngine) -> None:
        old_snap, new_snap, plan = _deliver_settlement_partitions(engine)
        result = engine.execute_settlement_export(
            plan.plan_id, [old_snap.partition_id, new_snap.partition_id],
            settlement_group="SETTLE-JOINT",
        )
        assert result["ok"] is True
        assert result["settlement_group"] == "SETTLE-JOINT"
        assert result["selection"] == ["amount"]  # 原选择
        details = {d["partition_range"]: d for d in result["review_details"]}
        # 复核详情：旧日期字段缺失，新日期零行，均不显示已完成
        assert details["20261121"]["projection_status"] == "字段缺失"
        assert details["20261121"]["missing_fields"] == ["amount"]
        assert details["20261121"]["actual_columns"] == ["id", "gross"]
        assert details["20261122"]["projection_status"] == "零行"
        assert details["20261122"]["hit_fields"] == ["amount"]
        # 合并出数只汇总字段命中的分区
        assert result["combined_rows"] == 0
        assert result["combined_columns"] == ["amount"]
        # 联合请求为两个日期各登记一条补采待办
        status = engine.settlement_posting_status("SETTLE-JOINT")
        assert sorted(status["waiting_dates"]) == ["20261121", "20261122"]

    def test_retry_after_interruption_uses_original_selection(
        self, engine: FusionEngine,
    ) -> None:
        """中断后重试要对应原选择。"""
        _, new_snap, plan = _deliver_settlement_partitions(engine)
        result = engine.retry_export_from_snapshot(plan.plan_id, new_snap.partition_id)
        assert result["ok"] is True
        assert result["retried"] is True
        assert result["selection"] == ["amount"]
        assert result["projection"]["requested_fields"] == ["amount"]
        assert result["projection"]["header"] == ["amount"]


class TestDeliveredCrossValidation:
    """用实际交付的分区做宽表交叉校验。"""

    def test_completed_and_waiting_scope(self, engine: FusionEngine) -> None:
        """只把本次有资格的日期和列与参照实际比较，记录完成/等待范围。"""
        old_snap, new_snap, plan = _deliver_settlement_partitions(engine)
        cv_plan = _reference_cv_plan(engine, ref_amount=15)
        engine.execute_settlement_export(
            plan.plan_id, [old_snap.partition_id, new_snap.partition_id],
            settlement_group="SETTLE-CV",
        )
        result = engine.execute_delivered_cross_validation(
            cv_plan.plan_id, "SETTLE-CV",
        )
        assert result["ok"] is True
        # 完成比较的范围：只有字段命中的新日期
        assert result["completed_scope"] == [
            {"partition_range": "20261122", "fields": ["amount"]},
        ]
        # 等待范围：缺列的旧日期
        assert result["waiting_scope"] == [
            {"partition_range": "20261121", "missing_fields": ["amount"]},
        ]
        assert result["mismatch_count"] == 0

    def test_missing_column_date_cannot_be_confirmed_consistent(
        self, engine: FusionEngine,
    ) -> None:
        """缺列的日期不能被确认一致。"""
        old_snap, new_snap, plan = _deliver_settlement_partitions(engine)
        cv_plan = _reference_cv_plan(engine, ref_amount=15)
        engine.execute_settlement_export(
            plan.plan_id, [old_snap.partition_id, new_snap.partition_id],
            settlement_group="SETTLE-CONS",
        )
        result = engine.execute_delivered_cross_validation(
            cv_plan.plan_id, "SETTLE-CONS",
        )
        assert result["validate_status"] == "校验中"
        assert result["can_confirm_consistent"] is False
        assert cv_plan.validate_status == "校验中"

    def test_cross_validation_requires_projection_audits(
        self, engine: FusionEngine,
    ) -> None:
        """交叉校验读取投影核对明细：没有明细时报错。"""
        cv_plan = _reference_cv_plan(engine)
        result = engine.execute_delivered_cross_validation(
            cv_plan.plan_id, "SETTLE-EMPTY",
        )
        assert result["ok"] is False
        assert "投影核对明细" in result["message"]


class TestBackfillTodoAndPosting:
    """补采待办办结与结算入账门禁。"""

    def test_pre_check_gates_todo_closing(self, engine: FusionEngine) -> None:
        """单日结果登记为预核后，下一班才能关闭该日期的补采待办。"""
        old_snap, new_snap, plan = _deliver_settlement_partitions(engine)
        engine.execute_settlement_export(
            plan.plan_id, [old_snap.partition_id, new_snap.partition_id],
            settlement_group="SETTLE-TODO",
        )
        # 未登记预核前不能关闭
        assert engine.close_backfill_todo("SETTLE-TODO", "20261122")["ok"] is False
        # 登记预核后下一班关闭
        pre = engine.register_day_pre_check("SETTLE-TODO", "20261122")
        assert pre["ok"] is True
        assert pre["review_status"] == "预核"
        close = engine.close_backfill_todo("SETTLE-TODO", "20261122")
        assert close["ok"] is True
        assert close["todo_status"] == "办结"

    def test_field_missing_date_cannot_pre_check(self, engine: FusionEngine) -> None:
        """未命中字段的分区不能算已核过的零行，不能登记预核。"""
        old_snap, new_snap, plan = _deliver_settlement_partitions(engine)
        engine.execute_settlement_export(
            plan.plan_id, [old_snap.partition_id, new_snap.partition_id],
            settlement_group="SETTLE-PRE",
        )
        result = engine.register_day_pre_check("SETTLE-PRE", "20261121")
        assert result["ok"] is False
        assert "字段缺失" in result["message"]

    def test_posting_status_reports_waiting_and_blockers(
        self, engine: FusionEngine,
    ) -> None:
        """值班核对：哪些日期办结、哪些等待，以及本次仍未入账的原因。"""
        old_snap, new_snap, plan = _deliver_settlement_partitions(engine)
        cv_plan = _reference_cv_plan(engine, ref_amount=15)
        engine.execute_settlement_export(
            plan.plan_id, [old_snap.partition_id, new_snap.partition_id],
            settlement_group="SETTLE-DUTY",
        )
        engine.execute_delivered_cross_validation(cv_plan.plan_id, "SETTLE-DUTY")
        engine.register_day_pre_check("SETTLE-DUTY", "20261122")
        engine.close_backfill_todo("SETTLE-DUTY", "20261122")

        status = engine.settlement_posting_status("SETTLE-DUTY")
        assert status["ok"] is True
        assert status["closed_dates"] == ["20261122"]
        assert status["waiting_dates"] == ["20261121"]
        assert status["can_post"] is False
        assert any("字段缺失" in blocker for blocker in status["post_blockers"])
        # 两个日期未均完成复核，不能交给结算员入账
        hand = engine.hand_to_settlement("SETTLE-DUTY")
        assert hand["ok"] is False
        assert hand["post_blockers"]


class TestConfirmSourceAndRerun:
    """为旧分区核定 amount 来源后用原选择重跑。"""

    def test_rerun_reports_difference_two(self, engine: FusionEngine) -> None:
        """旧分区原金额 17、参照同键金额 15，实际复核此时才报告差额 2。"""
        old_snap, new_snap, plan = _deliver_settlement_partitions(engine)
        cv_plan = _reference_cv_plan(engine, ref_amount=15)
        engine.execute_settlement_export(
            plan.plan_id, [old_snap.partition_id, new_snap.partition_id],
            settlement_group="SETTLE-RERUN",
        )
        first = engine.execute_delivered_cross_validation(
            cv_plan.plan_id, "SETTLE-RERUN",
        )
        # 旧日期缺列在等待，尚未比较，不报差额
        assert first["mismatch_count"] == 0
        assert [s["partition_range"] for s in first["waiting_scope"]] == ["20261121"]

        # 核定 amount 的来源（gross），用原选择重跑
        confirm = engine.confirm_partition_field_source(
            old_snap.partition_id, "amount", "gross",
        )
        assert confirm["ok"] is True
        rerun = engine.retry_export_from_snapshot(
            plan.plan_id, old_snap.partition_id, settlement_group="SETTLE-RERUN",
        )
        assert rerun["selection"] == ["amount"]
        assert rerun["projection"]["status"] == "已出数"
        assert rerun["projection"]["missing_fields"] == []
        # 重跑后同一日期只有一份投影核对明细
        audits = engine.projection_audits("SETTLE-RERUN")
        assert len([a for a in audits if a.partition_range == "20261121"]) == 1

        second = engine.execute_delivered_cross_validation(
            cv_plan.plan_id, "SETTLE-RERUN",
        )
        assert second["waiting_scope"] == []
        assert second["mismatch_count"] == 1
        mismatch = second["mismatches"][0]
        assert mismatch["partition_range"] == "20261121"
        assert mismatch["key"] == "k1"
        assert mismatch["field"] == "amount"
        assert mismatch["primary_value"] == 17
        assert mismatch["reference_value"] == 15
        assert mismatch["difference"] == 2
        assert second["validate_status"] == "偏差告警"

    def test_confirm_source_rejects_unknown_source_column(
        self, engine: FusionEngine,
    ) -> None:
        old_snap, _, _ = _deliver_settlement_partitions(engine)
        result = engine.confirm_partition_field_source(
            old_snap.partition_id, "amount", "nonexistent",
        )
        assert result["ok"] is False
        assert "nonexistent" in result["message"]


class TestFullSettlementFlow:
    """完整结算核对流程：联合导出 -> 交叉校验 -> 预核/办结 -> 复核 -> 入账。"""

    def test_posts_only_after_both_dates_reviewed(self, engine: FusionEngine) -> None:
        old_snap, new_snap, plan = _deliver_settlement_partitions(engine)
        cv_plan = _reference_cv_plan(engine, ref_amount=15)
        group = "SETTLE-FLOW"

        # 一次同时导出两个日期的 amount：先分别请求，再合起来出数并立即复核
        joint = engine.execute_settlement_export(
            plan.plan_id, [old_snap.partition_id, new_snap.partition_id],
            settlement_group=group,
        )
        details = {d["partition_range"]: d for d in joint["review_details"]}
        assert details["20261121"]["projection_status"] == "字段缺失"
        assert details["20261122"]["projection_status"] == "零行"

        # 两边字段定义冲突，立刻用实际交付的分区做宽表交叉校验
        first_cv = engine.execute_delivered_cross_validation(cv_plan.plan_id, group)
        assert first_cv["can_confirm_consistent"] is False

        # 单日（新日期）登记预核，下一班关闭该日期的补采待办
        assert engine.register_day_pre_check(group, "20261122")["ok"] is True
        assert engine.close_backfill_todo(group, "20261122")["ok"] is True
        # 新日期完成复核
        assert engine.complete_day_review(group, "20261122")["ok"] is True

        # 旧日期仍等待，不能入账
        assert engine.hand_to_settlement(group)["ok"] is False

        # 为旧分区核定 amount 来源后用原选择重跑，复核报告差额 2
        engine.confirm_partition_field_source(old_snap.partition_id, "amount", "gross")
        engine.retry_export_from_snapshot(
            plan.plan_id, old_snap.partition_id, settlement_group=group,
        )
        second_cv = engine.execute_delivered_cross_validation(cv_plan.plan_id, group)
        assert second_cv["mismatches"][0]["difference"] == 2

        # 旧日期复核完成后，两个日期均完成复核
        assert engine.register_day_pre_check(group, "20261121")["ok"] is True
        assert engine.close_backfill_todo(group, "20261121")["ok"] is True
        assert engine.complete_day_review(group, "20261121")["ok"] is True

        status = engine.settlement_posting_status(group)
        assert status["closed_dates"] == ["20261121", "20261122"]
        assert status["waiting_dates"] == []
        assert status["can_post"] is True

        # 交给结算员入账
        hand = engine.hand_to_settlement(group)
        assert hand["ok"] is True
        assert hand["posted"] is True
        # 重复交账被拒绝
        assert engine.hand_to_settlement(group)["ok"] is False
