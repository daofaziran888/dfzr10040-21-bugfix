"""熔炉引擎：跨模块数据融合流水线编排器。

负责执行数据源 -> Schema 映射 -> 宽表定义 -> 融合作业的完整链路，
并管理冲突仲裁、质量门禁、分区快照、水位追踪与增量归并。
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from app.models import (
    BackfillTask,
    ColdStoreStrategy,
    CrossValidatePlan,
    DataDriftRecord,
    ExportPlan,
    FusionJob,
    FusionMonitorMetric,
    LineageEdge,
    MergeConflictRule,
    MetricAggrRule,
    PartitionRule,
    ProjectionAudit,
    QualityGateRule,
    SchemaMapping,
    SnapshotPartition,
    SourceDef,
    WatermarkRecord,
    WideTableDef,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _generate_code(prefix: str, index: int) -> str:
    return f"{prefix}-{index:04d}"


def _numeric_difference(primary: Any, reference: Any) -> int | float | None:
    """计算数值差额（主体 - 参照）；非数值返回 None。"""
    try:
        diff = float(primary) - float(reference)
    except (TypeError, ValueError):
        return None
    return int(diff) if diff == int(diff) else diff


class FusionEngine:
    """熔炉引擎：编排跨模块数据融合流水线。"""

    def __init__(self) -> None:
        self._sources: dict[int, SourceDef] = {}
        self._schemas: dict[int, SchemaMapping] = {}
        self._wide_tables: dict[int, WideTableDef] = {}
        self._fusion_jobs: dict[int, FusionJob] = {}
        self._conflict_rules: dict[int, MergeConflictRule] = {}
        self._quality_gates: dict[int, QualityGateRule] = {}
        self._snapshots: dict[int, SnapshotPartition] = {}
        self._backfills: dict[int, BackfillTask] = {}
        self._cold_stores: dict[int, ColdStoreStrategy] = {}
        self._lineages: dict[int, LineageEdge] = {}
        self._metrics: dict[int, MetricAggrRule] = {}
        self._partitions: dict[int, PartitionRule] = {}
        self._watermarks: dict[int, WatermarkRecord] = {}
        self._exports: dict[int, ExportPlan] = {}
        self._cross_validates: dict[int, CrossValidatePlan] = {}
        self._drifts: dict[int, DataDriftRecord] = {}
        self._monitors: dict[int, FusionMonitorMetric] = {}
        self._projection_audits: dict[int, ProjectionAudit] = {}
        self._settlement_groups: dict[str, dict[str, Any]] = {}
        self._next_id: int = 1

    def _next_seq(self) -> int:
        sid = self._next_id
        self._next_id += 1
        return sid

    # ---- 1. source_def -> schema_map -> wide_table_def -> fusion_job ----

    def register_source(self, source_code: str, source_name: str, source_type: str,
                        connection_string: str, auth_method: str = "jdbc",
                        charset: str = "UTF-8", timezone: str = "Asia/Shanghai") -> SourceDef:
        sid = self._next_seq()
        source = SourceDef(
            source_id=sid, source_code=source_code, source_name=source_name,
            source_type=source_type, connection_string=connection_string,
            auth_method=auth_method, charset=charset, timezone=timezone,
            access_status="待验证",
        )
        self._sources[sid] = source
        return source

    def create_schema_mapping(self, source_table: str, target_wide_table: str,
                              field_mapping_json: str, type_conversion: str = "",
                              missing_policy: str = "skip") -> SchemaMapping:
        sid = self._next_seq()
        mapping = SchemaMapping(
            mapping_id=sid, plan_code=_generate_code("SCHE", sid),
            source_table=source_table, target_wide_table=target_wide_table,
            field_mapping_json=field_mapping_json,
            type_conversion_rules=type_conversion,
            missing_value_policy=missing_policy,
            mapping_order="ASC", plan_status="草稿",
        )
        self._schemas[sid] = mapping
        return mapping

    def define_wide_table(self, table_code: str, table_name: str,
                          field_list: list[str], primary_key: str = "id",
                          partition_key: str = "dt", ttl_days: int = 90) -> WideTableDef:
        sid = self._next_seq()
        wt = WideTableDef(
            table_id=sid, table_code=table_code, table_name=table_name,
            field_list=field_list, primary_key=primary_key,
            partition_key=partition_key, ttl_days=ttl_days,
            source_count=0, last_updated=_now_iso(),
        )
        self._wide_tables[sid] = wt
        return wt

    def create_fusion_job(self, target_wide_table: str, participant_sources: list[str],
                          join_key: str, fusion_window: str = "1d",
                          watermark_field: str = "update_time") -> FusionJob:
        sid = self._next_seq()
        job = FusionJob(
            job_id=sid, job_code=_generate_code("FUSI", sid),
            target_wide_table=target_wide_table,
            participant_sources=participant_sources,
            join_key=join_key, fusion_window=fusion_window,
            watermark_field=watermark_field,
            output_rows=0, skipped_rows=0,
        )
        self._fusion_jobs[sid] = job
        return job

    # ---- 2. fusion_job -> merge_conflict -> quality_gate -> snapshot_mgr ----

    def create_conflict_rule(self, target_wide_table: str, conflict_fields: list[str],
                             arbitration_strategy: str = "source_priority",
                             priority_source: str = "", time_baseline: str = "") -> MergeConflictRule:
        sid = self._next_seq()
        rule = MergeConflictRule(
            rule_id=sid, rule_code=_generate_code("MERG", sid),
            target_wide_table=target_wide_table,
            conflict_fields=conflict_fields,
            arbitration_strategy=arbitration_strategy,
            priority_source=priority_source,
            time_baseline=time_baseline or _now_iso(),
            conflict_count=0, rule_status="草稿",
        )
        self._conflict_rules[sid] = rule
        return rule

    def create_quality_gate(self, target_wide_table: str, check_type: str,
                            threshold_condition: str, trigger_action: str = "block",
                            block_level: str = "warning") -> QualityGateRule:
        sid = self._next_seq()
        gate = QualityGateRule(
            rule_id=sid, rule_code=_generate_code("QUAL", sid),
            target_wide_table=target_wide_table,
            check_type=check_type,
            threshold_condition=threshold_condition,
            trigger_action=trigger_action,
            block_level=block_level,
            checked_rows=0, last_triggered="",
        )
        self._quality_gates[sid] = gate
        return gate

    def create_snapshot(self, target_wide_table: str, partition_key_value: str,
                        dependency_partitions: list[str] | None = None,
                        schema_digest: str = "",
                        columns: list[str] | None = None,
                        rows: list[dict[str, Any]] | None = None) -> SnapshotPartition:
        sid = self._next_seq()
        snap = SnapshotPartition(
            partition_id=sid,
            partition_code=_generate_code("SNAP", sid),
            target_wide_table=target_wide_table,
            partition_key_value=partition_key_value,
            record_count=len(rows) if rows else 0,
            schema_digest=schema_digest or f"SHA256-{uuid.uuid4().hex[:8]}",
            status_mark="创建中",
            dependency_partitions=dependency_partitions or [],
            created_time=_now_iso(),
            columns=list(columns or []),
            rows=[dict(row) for row in (rows or [])],
        )
        self._snapshots[sid] = snap
        return snap

    # ---- 3. fusion_job -> watermark_track -> delta_merge ----

    def create_watermark(self, data_source: str, current_watermark: str,
                         data_origin: str = "fusion_job") -> WatermarkRecord:
        sid = self._next_seq()
        wm = WatermarkRecord(
            record_id=sid, record_code=_generate_code("WATE", sid),
            data_source=data_source,
            current_watermark=current_watermark,
            previous_watermark="",
            advance_interval="5m",
            delay_minutes=0,
            data_origin=data_origin,
            last_advance=_now_iso(),
        )
        self._watermarks[sid] = wm
        return wm

    def advance_watermark(self, record_id: int, new_watermark: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(record_id)
        if wm is None:
            return None
        wm.previous_watermark = wm.current_watermark
        wm.current_watermark = new_watermark
        wm.last_advance = _now_iso()
        return wm

    # ---- 4. snapshot_mgr -> backfill_task -> cold_store ----

    def create_backfill_task(self, target_wide_table: str, backfill_range: str,
                             parallel_partitions: int = 4, dependency_check: str = "",
                             checkpoint_position: str = "") -> BackfillTask:
        sid = self._next_seq()
        task = BackfillTask(
            task_id=sid, task_code=_generate_code("BACK", sid),
            target_wide_table=target_wide_table,
            backfill_range=backfill_range,
            parallel_partitions=parallel_partitions,
            dependency_check=dependency_check,
            checkpoint_position=checkpoint_position,
            processed_rows=0, remaining_rows=0,
        )
        self._backfills[sid] = task
        return task

    def create_cold_store_strategy(self, target_wide_table: str, hot_data_days: int,
                                   cold_storage_type: str = "parquet",
                                   compression_method: str = "zstd",
                                   migration_window: str = "02:00-06:00") -> ColdStoreStrategy:
        sid = self._next_seq()
        cs = ColdStoreStrategy(
            strategy_id=sid, strategy_code=_generate_code("COLD", sid),
            target_wide_table=target_wide_table,
            hot_data_days=hot_data_days,
            cold_storage_type=cold_storage_type,
            migration_window=migration_window,
            compression_method=compression_method,
            migrated_rows=0, last_migration="",
        )
        self._cold_stores[sid] = cs
        return cs

    # ---- 5. wide_table_def -> lineage_edge -> metric_aggr ----

    def create_lineage_edge(self, upstream_node: str, downstream_node: str,
                            related_fields: list[str], transform_function: str = "",
                            edge_type: str = "direct", dependency_level: int = 1) -> LineageEdge:
        sid = self._next_seq()
        edge = LineageEdge(
            edge_id=sid, edge_code=_generate_code("LINE", sid),
            upstream_node=upstream_node, downstream_node=downstream_node,
            related_fields=related_fields,
            transform_function=transform_function,
            edge_type=edge_type,
            dependency_level=dependency_level,
            last_traced=_now_iso(),
        )
        self._lineages[sid] = edge
        return edge

    def create_metric_aggr_rule(self, target_wide_table: str, agg_function: str,
                                group_dimensions: list[str], time_granularity: str = "1d",
                                precompute_window: str = "7d",
                                storage_table: str = "") -> MetricAggrRule:
        sid = self._next_seq()
        rule = MetricAggrRule(
            rule_id=sid, rule_code=_generate_code("METR", sid),
            target_wide_table=target_wide_table,
            agg_function=agg_function,
            group_dimensions=group_dimensions,
            time_granularity=time_granularity,
            precompute_window=precompute_window,
            storage_table=storage_table or f"aggr_{target_wide_table}",
            last_computed="",
        )
        self._metrics[sid] = rule
        return rule

    # ---- 6. partition_rule -> cold_store -> archive ----

    def create_partition_rule(self, target_wide_table: str, partition_type: str,
                              partition_field: str, retention_days: int = 30,
                              archive_policy: str = "move_to_cold") -> PartitionRule:
        sid = self._next_seq()
        rule = PartitionRule(
            rule_id=sid, strategy_code=_generate_code("PART", sid),
            target_wide_table=target_wide_table,
            partition_type=partition_type,
            partition_field=partition_field,
            retention_days=retention_days,
            archive_policy=archive_policy,
            partition_count=0,
            last_adjusted=_now_iso(),
        )
        self._partitions[sid] = rule
        return rule

    # ---- 7. snapshot_mgr -> export_snapshot ----

    def create_export_plan(self, target_wide_table: str, export_format: str = "parquet",
                           partition_filter: str = "", field_filter: list[str] | None = None,
                           export_path: str = "/exports", row_limit: int = 0) -> ExportPlan:
        sid = self._next_seq()
        plan = ExportPlan(
            plan_id=sid, plan_code=_generate_code("EXPO", sid),
            target_wide_table=target_wide_table,
            export_format=export_format,
            partition_filter=partition_filter,
            field_filter=field_filter or [],
            export_path=export_path,
            row_limit=row_limit,
            export_status="待导出",
        )
        self._exports[sid] = plan
        return plan

    # ---- 8. cross_validate -> data_drift -> fusion_monitor ----

    def create_cross_validate_plan(self, primary_wide_table: str,
                                   reference_wide_table: str, join_key: str,
                                   validate_fields: list[str],
                                   deviation_threshold: str = "0.05") -> CrossValidatePlan:
        sid = self._next_seq()
        plan = CrossValidatePlan(
            plan_id=sid, plan_code=_generate_code("CROS", sid),
            primary_wide_table=primary_wide_table,
            reference_wide_table=reference_wide_table,
            join_key=join_key,
            validate_fields=validate_fields,
            deviation_threshold=deviation_threshold,
            last_result="",
            validate_status="待校验",
        )
        self._cross_validates[sid] = plan
        return plan

    def create_drift_record(self, wide_table_name: str, drift_type: str,
                            historical_baseline: str, current_distribution: str,
                            kl_divergence: str = "0.0",
                            drift_direction: str = "none") -> DataDriftRecord:
        sid = self._next_seq()
        record = DataDriftRecord(
            record_id=sid, record_code=_generate_code("DATA", sid),
            wide_table_name=wide_table_name,
            drift_type=drift_type,
            historical_baseline=historical_baseline,
            current_distribution=current_distribution,
            kl_divergence=kl_divergence,
            drift_direction=drift_direction,
            detected_at=_now_iso(),
        )
        self._drifts[sid] = record
        return record

    def create_fusion_monitor(self, fusion_job: str, monitor_dimension: str,
                              collection_interval: str = "60s",
                              alert_threshold: str = "",
                              alert_channel: str = "webhook",
                              silence_window: str = "") -> FusionMonitorMetric:
        sid = self._next_seq()
        metric = FusionMonitorMetric(
            metric_id=sid, metric_code=_generate_code("FUSI", sid),
            fusion_job=fusion_job,
            monitor_dimension=monitor_dimension,
            collection_interval=collection_interval,
            alert_threshold=alert_threshold,
            alert_channel=alert_channel,
            silence_window=silence_window,
            last_sample=_now_iso(),
        )
        self._monitors[sid] = metric
        return metric

    # ---- 复合融合操作 ----

    def execute_fusion_pipeline(self, job_id: int) -> dict[str, Any]:
        """执行完整融合流水线：数据源读取 -> 映射转换 -> 冲突仲裁 -> 质量检查 -> 产出快照。

        返回值包含融合记录、冲突日志、质量报告和快照信息。
        """
        job = self._fusion_jobs.get(job_id)
        if job is None:
            return {"ok": False, "message": f"融合作业 {job_id} 不存在"}

        raw_records: list[dict[str, Any]] = []
        for source_code in job.participant_sources:
            source = self._find_source_by_code(source_code)
            if source is None:
                continue
            source_records = self._simulate_source_read(source)
            for rec in source_records:
                rec["_source"] = source_code
                rec["_fusion_key"] = f"{source_code}#{rec.get(job.join_key, '')}"
                rec["_timestamp"] = _now_iso()
                rec["_version"] = 1
            raw_records.extend(source_records)

        mapped_records = self._apply_schema_mappings(raw_records, job.target_wide_table)

        job.output_rows = len(mapped_records)
        job.skipped_rows = len(raw_records) - len(mapped_records)
        job.merged_records = mapped_records

        result: dict[str, Any] = {
            "ok": True,
            "job_code": job.job_code,
            "total_sources": len(job.participant_sources),
            "raw_records": len(raw_records),
            "mapped_records": len(mapped_records),
            "skipped_records": job.skipped_rows,
            "records": mapped_records,
        }

        conflict_result = self._apply_conflict_resolution(mapped_records, job.target_wide_table)
        result["conflict_resolution"] = conflict_result

        quality_result = self._apply_quality_gates(
            conflict_result.get("resolved", mapped_records),
            job.target_wide_table,
        )
        result["quality_gate"] = quality_result

        if quality_result.get("passed", True):
            snap = self.create_snapshot(
                job.target_wide_table,
                partition_key_value=datetime.now(timezone.utc).strftime("%Y%m%d"),
            )
            snap.record_count = len(conflict_result.get("resolved", mapped_records))
            result["snapshot"] = {"partition_code": snap.partition_code, "record_count": snap.record_count}

        return result

    def execute_delta_merge(self, job_id: int, delta_records: list[dict[str, Any]],
                            watermark_record_id: int = 0) -> dict[str, Any]:
        """执行增量归并：将增量数据合并到融合作业的输出中。"""
        job = self._fusion_jobs.get(job_id)
        if job is None:
            return {"ok": False, "message": f"融合作业 {job_id} 不存在"}

        existing = {self._make_merge_key(r, job.join_key): r for r in job.merged_records}
        inserted = 0
        updated = 0
        deleted = 0

        for delta in delta_records:
            key = self._make_merge_key(delta, job.join_key)
            op = delta.get("_op", "upsert")
            if op == "delete":
                if key in existing:
                    del existing[key]
                    deleted += 1
            elif key in existing:
                existing[key].update(delta)
                updated += 1
            else:
                existing[key] = dict(delta)
                inserted += 1

        job.merged_records = list(existing.values())

        if watermark_record_id > 0:
            wm = self._watermarks.get(watermark_record_id)
            if wm is not None:
                wm.previous_watermark = wm.current_watermark
                wm.current_watermark = _now_iso()

        return {
            "ok": True,
            "inserted": inserted,
            "updated": updated,
            "deleted": deleted,
            "total_after_merge": len(job.merged_records),
        }

    def execute_backfill_with_checkpoint(self, backfill_id: int,
                                         source_records: list[dict[str, Any]]) -> dict[str, Any]:
        """执行带断点续传的数据回填。

        从 checkpoint_position 处继续处理，失败时保留断点可恢复。
        """
        task = self._backfills.get(backfill_id)
        if task is None:
            return {"ok": False, "message": f"回填任务 {backfill_id} 不存在"}

        start_idx = 0
        if task.checkpoint_position:
            try:
                start_idx = int(task.checkpoint_position)
            except (ValueError, TypeError):
                start_idx = 0

        batch_size = max(task.parallel_partitions * 100, 100)
        end_idx = min(start_idx + batch_size, len(source_records))
        batch = source_records[start_idx:end_idx]

        processed = len(batch)
        task.processed_rows += processed
        task.remaining_rows = max(0, len(source_records) - end_idx)

        if end_idx < len(source_records):
            task.checkpoint_position = str(end_idx)
            complete = False
        else:
            task.checkpoint_position = ""
            complete = True

        return {
            "ok": True,
            "batch_processed": processed,
            "total_processed": task.processed_rows,
            "remaining": task.remaining_rows,
            "checkpoint": task.checkpoint_position,
            "complete": complete,
        }

    def migrate_to_cold_store(self, strategy_id: int,
                              hot_partition_cutoff: str) -> dict[str, Any]:
        """执行冷热分层迁移：将超过热数据保留期的分区迁移到冷存储。"""
        strategy = self._cold_stores.get(strategy_id)
        if strategy is None:
            return {"ok": False, "message": f"冷热分层策略 {strategy_id} 不存在"}

        eligible_snaps = [
            snap for snap in self._snapshots.values()
            if snap.target_wide_table == strategy.target_wide_table
            and snap.partition_key_value < hot_partition_cutoff
        ]
        migrated = 0
        for snap in eligible_snaps:
            migrated += snap.record_count
            snap.status_mark = "已迁移至冷存储"

        strategy.migrated_rows += migrated
        strategy.last_migration = _now_iso()

        return {
            "ok": True,
            "migrated_partitions": len(eligible_snaps),
            "migrated_rows": migrated,
            "strategy_code": strategy.strategy_code,
        }

    def execute_export_from_snapshot(self, export_id: int,
                                     snapshot_id: int,
                                     settlement_group: str = "") -> dict[str, Any]:
        """从分区快照执行数据导出，并按分区实际列定义输出投影核对明细。

        空投影判断：只有分区确实零行且所选字段全部命中时才算有效空明细
        （带真实列名和分区范围）；字段未命中的分区记录字段缺失，
        不再返回一串空对象冒充已完成。
        """
        plan = self._exports.get(export_id)
        snap = self._snapshots.get(snapshot_id)
        if plan is None:
            return {"ok": False, "message": f"导出计划 {export_id} 不存在"}
        if snap is None:
            return {"ok": False, "message": f"分区快照 {snapshot_id} 不存在"}

        records = self._partition_records(snap)
        actual_columns = self._partition_columns(snap, records)

        requested = list(plan.field_filter)
        if requested:
            hit = [f for f in requested if f in actual_columns]
            missing = [f for f in requested if f not in actual_columns]
        else:
            hit, missing = [], []

        if requested:
            # 只投影命中列；全部未命中时不出空对象
            projected = [
                {k: v for k, v in rec.items() if k in hit}
                for rec in records
            ] if hit else []
        else:
            projected = [dict(rec) for rec in records]

        if plan.row_limit > 0 and len(projected) > plan.row_limit:
            projected = projected[:plan.row_limit]

        if missing:
            projection_status = "字段缺失"
        elif not projected:
            projection_status = "零行"
        else:
            projection_status = "已出数"

        plan.export_status = "字段缺失" if missing else "已完成"

        header = hit if requested else actual_columns

        if settlement_group:
            self._drop_stale_audits(settlement_group, snap.partition_key_value)
        audit = ProjectionAudit(
            audit_id=self._next_seq(),
            plan_code=plan.plan_code,
            snapshot_id=snap.partition_id,
            partition_code=snap.partition_code,
            partition_range=snap.partition_key_value,
            requested_fields=requested,
            actual_columns=actual_columns,
            hit_fields=hit,
            missing_fields=missing,
            row_count=len(projected),
            status=projection_status,
            settlement_group=settlement_group,
        )
        self._projection_audits[audit.audit_id] = audit
        if settlement_group:
            self._ensure_settlement_todo(settlement_group, snap.partition_key_value)

        return {
            "ok": True,
            "plan_code": plan.plan_code,
            "snapshot_code": snap.partition_code,
            "exported_rows": len(projected),
            "format": plan.export_format,
            "path": plan.export_path,
            "records": projected,
            "projection": {
                "audit_id": audit.audit_id,
                "partition_range": snap.partition_key_value,
                "requested_fields": requested,
                "actual_columns": actual_columns,
                "hit_fields": hit,
                "missing_fields": missing,
                "header": header,
                "status": projection_status,
            },
        }

    def retry_export_from_snapshot(self, export_id: int, snapshot_id: int,
                                   settlement_group: str = "") -> dict[str, Any]:
        """中断后重试：沿用计划登记的原选择（字段筛选），不更换字段集合。"""
        plan = self._exports.get(export_id)
        if plan is None:
            return {"ok": False, "message": f"导出计划 {export_id} 不存在"}
        result = self.execute_export_from_snapshot(
            export_id, snapshot_id, settlement_group=settlement_group,
        )
        if result.get("ok"):
            result["retried"] = True
            result["selection"] = list(plan.field_filter)
        return result

    # ---- 结算核对：联合导出 -> 交付分区交叉校验 -> 补采待办 -> 入账 ----

    def deliver_partition_data(self, snapshot_id: int, columns: list[str],
                               rows: list[dict[str, Any]]) -> dict[str, Any]:
        """交付分区实际数据：列定义与记录行，作为导出投影与交叉校验的真源。"""
        snap = self._snapshots.get(snapshot_id)
        if snap is None:
            return {"ok": False, "message": f"分区快照 {snapshot_id} 不存在"}
        snap.columns = list(columns)
        snap.rows = [dict(row) for row in rows]
        snap.record_count = len(rows)
        return {
            "ok": True,
            "partition_code": snap.partition_code,
            "columns": list(snap.columns),
            "record_count": snap.record_count,
        }

    def execute_settlement_export(self, export_id: int, snapshot_ids: list[int],
                                  settlement_group: str = "") -> dict[str, Any]:
        """联合导出：先分别请求各分区，再合起来出数并立即复核。

        同一次结算核对的多个日期共用一个导出计划（原选择），每个分区
        单独出投影核对明细；合并出数只汇总本次字段命中的分区，
        复核详情按分区实际列定义给出，字段缺失不显示为已完成。
        """
        plan = self._exports.get(export_id)
        if plan is None:
            return {"ok": False, "message": f"导出计划 {export_id} 不存在"}
        group = settlement_group or f"SETTLE-{export_id:04d}"

        # 第一阶段：分别请求各分区，按分区实际列定义出投影核对明细
        per_partition: list[dict[str, Any]] = []
        for snapshot_id in snapshot_ids:
            per_partition.append(
                self.execute_export_from_snapshot(
                    export_id, snapshot_id, settlement_group=group,
                )
            )

        # 第二阶段：合起来出数并立即复核
        combined_records: list[dict[str, Any]] = []
        combined_columns: list[str] = []
        review_details: list[dict[str, Any]] = []
        for result in per_partition:
            if not result.get("ok"):
                review_details.append({"ok": False, "message": result.get("message", "")})
                continue
            projection = result["projection"]
            audit = self._projection_audits[projection["audit_id"]]
            if not projection["missing_fields"]:
                for field_name in projection["header"]:
                    if field_name not in combined_columns:
                        combined_columns.append(field_name)
                combined_records.extend(result["records"])
            review_details.append({
                "ok": True,
                "snapshot_code": result["snapshot_code"],
                "partition_range": projection["partition_range"],
                "projection_status": projection["status"],
                "actual_columns": projection["actual_columns"],
                "hit_fields": projection["hit_fields"],
                "missing_fields": projection["missing_fields"],
                "row_count": result["exported_rows"],
                "review_status": audit.review_status,
            })

        return {
            "ok": True,
            "plan_code": plan.plan_code,
            "settlement_group": group,
            "selection": list(plan.field_filter),
            "combined_rows": len(combined_records),
            "combined_columns": combined_columns,
            "records": combined_records,
            "review_details": review_details,
        }

    def confirm_partition_field_source(self, snapshot_id: int, field: str,
                                       source_field: str) -> dict[str, Any]:
        """为分区核定字段来源：从已有列派生目标列（如 amount 来源于 gross）。"""
        snap = self._snapshots.get(snapshot_id)
        if snap is None:
            return {"ok": False, "message": f"分区快照 {snapshot_id} 不存在"}
        if source_field not in snap.columns:
            return {"ok": False,
                    "message": f"分区 {snap.partition_code} 无来源列 {source_field}"}
        for row in snap.rows:
            row[field] = row.get(source_field)
        if field not in snap.columns:
            snap.columns.append(field)
        return {
            "ok": True,
            "partition_code": snap.partition_code,
            "field": field,
            "source_field": source_field,
        }

    def execute_delivered_cross_validation(self, plan_id: int,
                                           settlement_group: str) -> dict[str, Any]:
        """用实际交付的分区做宽表交叉校验。

        读取投影核对明细：只把本次有资格（字段命中）的日期和列与参照实际
        比较，记录完成比较的范围和等待范围；缺列的日期不能被确认一致。
        """
        plan = self._cross_validates.get(plan_id)
        if plan is None:
            return {"ok": False, "message": f"交叉校验方案 {plan_id} 不存在"}

        audits = [
            audit for audit in self._projection_audits.values()
            if audit.settlement_group == settlement_group
        ]
        if not audits:
            return {"ok": False,
                    "message": f"结算核对 {settlement_group} 没有投影核对明细"}

        ref_job = self._find_fusion_job_by_table(plan.reference_wide_table)
        ref_records = ref_job.merged_records if ref_job else []
        ref_by_key = {str(r.get(plan.join_key, "")): r for r in ref_records}

        mismatches: list[dict[str, Any]] = []
        completed_scope: list[dict[str, Any]] = []
        waiting_scope: list[dict[str, Any]] = []
        compared_keys = 0

        for audit in sorted(audits, key=lambda a: a.partition_range):
            if audit.missing_fields:
                audit.compared = False
                waiting_scope.append({
                    "partition_range": audit.partition_range,
                    "missing_fields": list(audit.missing_fields),
                })
                continue
            snap = self._snapshots.get(audit.snapshot_id)
            if snap is None:
                continue
            rows = self._partition_records(snap)
            columns = [f for f in plan.validate_fields if f in audit.hit_fields]
            for row in rows:
                key = str(row.get(plan.join_key, ""))
                ref = ref_by_key.get(key)
                if ref is None:
                    continue
                compared_keys += 1
                for field_name in columns:
                    pv = row.get(field_name)
                    rv = ref.get(field_name)
                    if str(pv) != str(rv):
                        mismatch: dict[str, Any] = {
                            "partition_range": audit.partition_range,
                            "key": key,
                            "field": field_name,
                            "primary_value": pv,
                            "reference_value": rv,
                        }
                        difference = _numeric_difference(pv, rv)
                        if difference is not None:
                            mismatch["difference"] = difference
                        mismatches.append(mismatch)
            audit.compared = True
            completed_scope.append({
                "partition_range": audit.partition_range,
                "fields": columns,
            })

        if mismatches:
            plan.validate_status = "偏差告警"
        elif waiting_scope:
            plan.validate_status = "校验中"  # 缺列日期仍在等待，不能确认一致
        else:
            plan.validate_status = "一致"

        plan.last_result = json.dumps({
            "settlement_group": settlement_group,
            "compared_keys": compared_keys,
            "mismatch_count": len(mismatches),
            "completed_scope": completed_scope,
            "waiting_scope": waiting_scope,
        }, ensure_ascii=False)

        return {
            "ok": True,
            "plan_code": plan.plan_code,
            "settlement_group": settlement_group,
            "validate_status": plan.validate_status,
            "compared_keys": compared_keys,
            "mismatch_count": len(mismatches),
            "mismatches": mismatches[:20],
            "completed_scope": completed_scope,
            "waiting_scope": waiting_scope,
            "can_confirm_consistent": not mismatches and not waiting_scope,
        }

    def register_day_pre_check(self, settlement_group: str,
                               partition_range: str) -> dict[str, Any]:
        """单日结果登记为预核：下一班可据此关闭该日期的补采待办。"""
        audit = self._find_audit(settlement_group, partition_range)
        if audit is None:
            return {"ok": False,
                    "message": f"{settlement_group} 没有分区 {partition_range} 的投影核对明细"}
        if audit.missing_fields:
            return {"ok": False,
                    "message": f"分区 {partition_range} 字段缺失"
                               f"（{'、'.join(audit.missing_fields)}），不能登记预核"}
        audit.review_status = "预核"
        return {
            "ok": True,
            "partition_range": partition_range,
            "review_status": audit.review_status,
            "message": f"分区 {partition_range} 已登记预核",
        }

    def complete_day_review(self, settlement_group: str,
                            partition_range: str) -> dict[str, Any]:
        """完成单日复核：需先登记预核，且本次交叉校验已覆盖该日期。"""
        audit = self._find_audit(settlement_group, partition_range)
        if audit is None:
            return {"ok": False,
                    "message": f"{settlement_group} 没有分区 {partition_range} 的投影核对明细"}
        if audit.missing_fields:
            return {"ok": False,
                    "message": f"分区 {partition_range} 字段缺失，不能完成复核"}
        if audit.review_status == "待复核":
            return {"ok": False,
                    "message": f"分区 {partition_range} 尚未登记预核"}
        if not audit.compared:
            return {"ok": False,
                    "message": f"分区 {partition_range} 尚未完成交叉校验比较"}
        audit.review_status = "已复核"
        return {
            "ok": True,
            "partition_range": partition_range,
            "review_status": audit.review_status,
            "message": f"分区 {partition_range} 已完成复核",
        }

    def close_backfill_todo(self, settlement_group: str,
                            partition_range: str) -> dict[str, Any]:
        """关闭该日期的补采待办：需单日结果已登记预核。"""
        state = self._settlement_groups.get(settlement_group)
        if state is None or partition_range not in state["todos"]:
            return {"ok": False,
                    "message": f"{settlement_group} 没有分区 {partition_range} 的补采待办"}
        audit = self._find_audit(settlement_group, partition_range)
        if audit is None or audit.review_status not in ("预核", "已复核"):
            return {"ok": False,
                    "message": f"分区 {partition_range} 尚未登记预核，补采待办不能关闭"}
        state["todos"][partition_range] = "办结"
        return {
            "ok": True,
            "partition_range": partition_range,
            "todo_status": "办结",
            "message": f"分区 {partition_range} 的补采待办已办结",
        }

    def settlement_posting_status(self, settlement_group: str) -> dict[str, Any]:
        """核对本次联合请求各日期办结/等待状态，以及本次仍未入账的原因。"""
        state = self._settlement_groups.get(settlement_group)
        audits = [
            audit for audit in self._projection_audits.values()
            if audit.settlement_group == settlement_group
        ]
        if state is None and not audits:
            return {"ok": False, "message": f"结算核对 {settlement_group} 不存在"}

        todos = state["todos"] if state else {}
        dates: list[dict[str, Any]] = []
        blockers: list[str] = []
        for audit in sorted(audits, key=lambda a: a.partition_range):
            dates.append({
                "partition_range": audit.partition_range,
                "projection_status": audit.status,
                "missing_fields": list(audit.missing_fields),
                "review_status": audit.review_status,
                "todo_status": todos.get(audit.partition_range, "待办"),
                "compared": audit.compared,
            })
            if audit.review_status != "已复核":
                if audit.missing_fields:
                    blockers.append(
                        f"分区 {audit.partition_range} 字段缺失"
                        f"（{'、'.join(audit.missing_fields)}），未完成复核"
                    )
                elif not audit.compared:
                    blockers.append(
                        f"分区 {audit.partition_range} 尚未完成交叉校验比较"
                    )
                else:
                    blockers.append(
                        f"分区 {audit.partition_range} 复核未完成（当前：{audit.review_status}）"
                    )

        return {
            "ok": True,
            "settlement_group": settlement_group,
            "dates": dates,
            "closed_dates": [d["partition_range"] for d in dates
                             if d["todo_status"] == "办结"],
            "waiting_dates": [d["partition_range"] for d in dates
                              if d["todo_status"] != "办结"],
            "can_post": bool(dates) and not blockers,
            "post_blockers": blockers,
            "posted": bool(state and state["posted"]),
        }

    def hand_to_settlement(self, settlement_group: str) -> dict[str, Any]:
        """两个日期均完成复核后，交给结算员入账。"""
        status = self.settlement_posting_status(settlement_group)
        if not status.get("ok"):
            return status
        if status["posted"]:
            return {"ok": False,
                    "message": f"结算核对 {settlement_group} 已入账，请勿重复交账"}
        if not status["can_post"]:
            return {
                "ok": False,
                "message": "；".join(status["post_blockers"]) or "复核未完成，暂不能入账",
                "post_blockers": status["post_blockers"],
            }
        state = self._settlement_groups.setdefault(
            settlement_group, {"todos": {}, "posted": False},
        )
        state["posted"] = True
        return {
            "ok": True,
            "settlement_group": settlement_group,
            "posted": True,
            "message": "已交给结算员入账",
        }

    def projection_audits(self, settlement_group: str | None = None) -> list[ProjectionAudit]:
        """投影核对明细清单：可按结算核对组过滤。"""
        audits = list(self._projection_audits.values())
        if settlement_group is not None:
            audits = [a for a in audits if a.settlement_group == settlement_group]
        return sorted(audits, key=lambda a: (a.partition_range, a.audit_id))

    def execute_cross_validation(self, plan_id: int) -> dict[str, Any]:
        """执行跨宽表交叉校验，检测数据一致性偏差。"""
        plan = self._cross_validates.get(plan_id)
        if plan is None:
            return {"ok": False, "message": f"交叉校验方案 {plan_id} 不存在"}

        primary_job = self._find_fusion_job_by_table(plan.primary_wide_table)
        ref_job = self._find_fusion_job_by_table(plan.reference_wide_table)

        primary_records = primary_job.merged_records if primary_job else []
        ref_records = ref_job.merged_records if ref_job else []

        primary_by_key = {str(r.get(plan.join_key, "")): r for r in primary_records}
        ref_by_key = {str(r.get(plan.join_key, "")): r for r in ref_records}

        mismatches: list[dict[str, Any]] = []
        drift_detected = False

        for key in set(primary_by_key.keys()) & set(ref_by_key.keys()):
            p = primary_by_key[key]
            r = ref_by_key[key]
            for field in plan.validate_fields:
                pv = str(p.get(field, ""))
                rv = str(r.get(field, ""))
                if pv != rv:
                    mismatches.append({
                        "key": key, "field": field,
                        "primary_value": pv, "reference_value": rv,
                    })
                    drift_detected = True

        plan.validate_status = "偏差告警" if mismatches else "一致"
        plan.last_result = json.dumps({
            "matched_keys": len(set(primary_by_key.keys()) & set(ref_by_key.keys())),
            "mismatch_count": len(mismatches),
            "drift_detected": drift_detected,
        }, ensure_ascii=False)

        result: dict[str, Any] = {
            "ok": True,
            "plan_code": plan.plan_code,
            "validate_status": plan.validate_status,
            "mismatch_count": len(mismatches),
            "drift_detected": drift_detected,
            "mismatches": mismatches[:20],
        }

        if drift_detected:
            drift_record = self.create_drift_record(
                wide_table_name=plan.primary_wide_table,
                drift_type="schema_mismatch",
                historical_baseline="baseline_" + _now_iso(),
                current_distribution="current_" + _now_iso(),
            )
            result["drift_record_code"] = drift_record.record_code

        return result

    def reconcile_partition_archive(self, partition_rule_id: int,
                                    cold_strategy_id: int) -> dict[str, Any]:
        """根据分区策略和冷热分层策略执行归档。

        将超出保留期的分区数据从冷存储转移到归档。
        """
        part_rule = self._partitions.get(partition_rule_id)
        cold_strat = self._cold_stores.get(cold_strategy_id)

        if part_rule is None or cold_strat is None:
            return {"ok": False, "message": "分区策略或冷热分层策略不存在"}

        eligible_snaps = [
            snap for snap in self._snapshots.values()
            if snap.target_wide_table == part_rule.target_wide_table
            and snap.status_mark == "已迁移至冷存储"
        ]

        archived = 0
        for snap in eligible_snaps:
            snap.status_mark = "已归档"
            archived += snap.record_count

        return {
            "ok": True,
            "archived_partitions": len(eligible_snaps),
            "archived_rows": archived,
            "archive_policy": part_rule.archive_policy,
        }

    def reconcile_watermark_with_fusion(self, job_id: int) -> dict[str, Any]:
        """协调水位线与融合作业的推进状态。

        水位延迟时推进受限，融合作业未完成时增量归并暂停。
        """
        job = self._fusion_jobs.get(job_id)
        if job is None:
            return {"ok": False, "message": f"融合作业 {job_id} 不存在"}

        related_watermarks = [
            wm for wm in self._watermarks.values()
            if wm.data_source in job.participant_sources
        ]

        stalled = [wm for wm in related_watermarks if wm.delay_minutes > 30]
        advancing = [wm for wm in related_watermarks if wm.delay_minutes <= 30]

        return {
            "ok": True,
            "job_code": job.job_code,
            "total_watermarks": len(related_watermarks),
            "advancing": len(advancing),
            "stalled": len(stalled),
            "can_proceed": len(stalled) == 0,
        }

    def build_wide_table_from_fusion(self, wide_table_id: int, fusion_job_id: int) -> dict[str, Any]:
        """从融合作业结果构建宽表定义，产生数据血缘边。"""
        wt = self._wide_tables.get(wide_table_id)
        job = self._fusion_jobs.get(fusion_job_id)

        if wt is None:
            return {"ok": False, "message": f"宽表定义 {wide_table_id} 不存在"}
        if job is None:
            return {"ok": False, "message": f"融合作业 {fusion_job_id} 不存在"}

        wt.source_count = len(job.participant_sources)
        wt.last_updated = _now_iso()

        lineages: list[str] = []
        for source_code in job.participant_sources:
            edge = self.create_lineage_edge(
                upstream_node=source_code,
                downstream_node=wt.table_name,
                related_fields=wt.field_list,
                edge_type="fusion_map",
                dependency_level=1,
            )
            lineages.append(edge.edge_code)

        return {
            "ok": True,
            "table_code": wt.table_code,
            "source_count": wt.source_count,
            "lineage_edges": lineages,
            "last_updated": wt.last_updated,
        }

    def compute_metric_from_lineage(self, lineage_edge_id: int,
                                    target_wide_table: str,
                                    agg_function: str,
                                    group_dimensions: list[str]) -> dict[str, Any]:
        """基于数据血缘边计算指标聚合规则。"""
        edge = self._lineages.get(lineage_edge_id)
        if edge is None:
            return {"ok": False, "message": f"数据血缘边 {lineage_edge_id} 不存在"}

        rule = self.create_metric_aggr_rule(
            target_wide_table=target_wide_table,
            agg_function=agg_function,
            group_dimensions=group_dimensions,
        )
        rule.last_computed = _now_iso()

        return {
            "ok": True,
            "metric_rule_code": rule.rule_code,
            "from_lineage": edge.edge_code,
            "agg_function": agg_function,
            "group_dimensions": group_dimensions,
        }

    # ---- 辅助方法 ----

    def _partition_records(self, snap: SnapshotPartition) -> list[dict[str, Any]]:
        """分区实际交付的记录：优先交付数据，否则回退到融合作业输出。"""
        if snap.columns or snap.rows:
            return list(snap.rows)
        job = self._find_fusion_job_by_table(snap.target_wide_table)
        return list(job.merged_records) if job else []

    def _partition_columns(self, snap: SnapshotPartition,
                           records: list[dict[str, Any]]) -> list[str]:
        """分区实际列定义：交付列优先，否则从记录键推导（排除内部键）。"""
        if snap.columns:
            return list(snap.columns)
        columns: list[str] = []
        for rec in records:
            for key in rec:
                if key.startswith("_"):
                    continue
                if key not in columns:
                    columns.append(key)
        return columns

    def _ensure_settlement_todo(self, settlement_group: str,
                                partition_range: str) -> None:
        """联合请求每覆盖一个日期，就登记一条补采待办。"""
        state = self._settlement_groups.setdefault(
            settlement_group, {"todos": {}, "posted": False},
        )
        state["todos"].setdefault(partition_range, "待办")

    def _drop_stale_audits(self, settlement_group: str,
                           partition_range: str) -> None:
        """同一结算核对组内重跑同一日期时，旧的投影核对明细作废。"""
        stale = [
            audit_id for audit_id, audit in self._projection_audits.items()
            if audit.settlement_group == settlement_group
            and audit.partition_range == partition_range
        ]
        for audit_id in stale:
            del self._projection_audits[audit_id]

    def _find_audit(self, settlement_group: str,
                    partition_range: str) -> ProjectionAudit | None:
        for audit in self._projection_audits.values():
            if (audit.settlement_group == settlement_group
                    and audit.partition_range == partition_range):
                return audit
        return None

    def _find_source_by_code(self, source_code: str) -> SourceDef | None:
        for source in self._sources.values():
            if source.source_code == source_code:
                return source
        return None

    def _find_fusion_job_by_table(self, wide_table_name: str) -> FusionJob | None:
        for job in self._fusion_jobs.values():
            if job.target_wide_table == wide_table_name:
                return job
        return None

    def _simulate_source_read(self, source: SourceDef) -> list[dict[str, Any]]:
        """模拟从数据源读取样本记录。"""
        return [
            {"id": f"{source.source_code}-rec-1", "name": f"{source.source_name}-rec1",
             "value": 100.0, "update_time": _now_iso()},
            {"id": f"{source.source_code}-rec-2", "name": f"{source.source_name}-rec2",
             "value": 200.0, "update_time": _now_iso()},
            {"id": f"{source.source_code}-rec-3", "name": f"{source.source_name}-rec3",
             "value": 300.0, "update_time": _now_iso()},
        ]

    def _apply_schema_mappings(self, records: list[dict[str, Any]],
                               target_wide_table: str) -> list[dict[str, Any]]:
        """应用 Schema 映射：对关联到目标宽表的映射方案进行字段转换。"""
        relevant = [
            m for m in self._schemas.values()
            if m.target_wide_table == target_wide_table
        ]
        if not relevant:
            return list(records)

        mapped: list[dict[str, Any]] = []
        for rec in records:
            source_table = rec.get("_source", "")
            applicable = [m for m in relevant if m.source_table == source_table]
            if applicable:
                new_rec = dict(rec)
                for mapping in applicable:
                    try:
                        field_map = json.loads(mapping.field_mapping_json)
                        for src_field, tgt_field in field_map.items():
                            if src_field in new_rec:
                                new_rec[tgt_field] = new_rec.pop(src_field)
                    except (json.JSONDecodeError, KeyError):
                        pass
                mapped.append(new_rec)
            else:
                mapped.append(rec)
        return mapped

    def _apply_conflict_resolution(self, records: list[dict[str, Any]],
                                   target_wide_table: str) -> dict[str, Any]:
        """应用冲突仲裁规则到融合记录。"""
        rules = [
            r for r in self._conflict_rules.values()
            if r.target_wide_table == target_wide_table and r.rule_status == "生效中"
        ]
        if not rules:
            return {"applied": False, "reason": "无生效的冲突规则", "resolved": records}

        resolved = list(records)
        all_conflicts: list[dict[str, Any]] = []
        for rule in rules:
            resolved, conflicts = rule.resolve(resolved)
            rule.conflict_count = len(conflicts)
            all_conflicts.extend(conflicts)

        return {
            "applied": True,
            "rules_used": len(rules),
            "conflicts_found": len(all_conflicts),
            "conflicts": all_conflicts[:50],
            "resolved": resolved,
        }

    def _apply_quality_gates(self, records: list[dict[str, Any]],
                             target_wide_table: str) -> dict[str, Any]:
        """应用质量门禁检查。"""
        gates = [
            g for g in self._quality_gates.values()
            if g.target_wide_table == target_wide_table
        ]
        if not gates:
            return {"passed": True, "gates_checked": 0, "violations": []}

        violations: list[dict[str, Any]] = []
        for gate in gates:
            gate.checked_rows = len(records)
            if gate.check_type == "row_count_min" and len(records) < int(gate.threshold_condition):
                violations.append({"rule_code": gate.rule_code, "check": "row_count_min",
                                   "actual": len(records), "threshold": gate.threshold_condition})
            elif gate.check_type == "null_check":
                null_count = sum(
                    1 for rec in records
                    if rec.get(gate.threshold_condition) is None
                )
                if null_count > 0:
                    violations.append({"rule_code": gate.rule_code, "check": "null_check",
                                       "null_count": null_count})

        passed = len(violations) == 0
        return {"passed": passed, "gates_checked": len(gates), "violations": violations}

    @staticmethod
    def _make_merge_key(record: dict[str, Any], join_key: str) -> str:
        return f"{record.get('_source', '_')}#{record.get(join_key, '_')}"

    def domain_snapshot(self) -> dict[str, Any]:
        """输出引擎全局数据质量快照。"""
        return {
            "sources": len(self._sources),
            "schemas": len(self._schemas),
            "wide_tables": len(self._wide_tables),
            "fusion_jobs": len(self._fusion_jobs),
            "conflict_rules": len(self._conflict_rules),
            "quality_gates": len(self._quality_gates),
            "snapshots": len(self._snapshots),
            "backfills": len(self._backfills),
            "cold_stores": len(self._cold_stores),
            "lineages": len(self._lineages),
            "metrics": len(self._metrics),
            "partitions": len(self._partitions),
            "watermarks": len(self._watermarks),
            "exports": len(self._exports),
            "cross_validates": len(self._cross_validates),
            "drifts": len(self._drifts),
            "monitors": len(self._monitors),
            "projection_audits": len(self._projection_audits),
            "settlement_groups": len(self._settlement_groups),
        }


# 全局单例供测试和跨模块消费
fusion_engine = FusionEngine()