#!/usr/bin/env python3
"""
全Run対象のフォールバックポート一括生成スクリプト

YAMLファイルがない（またはアクセスできない）全Runに対して、
Edgesテーブルから推測してPorts/PortConnectionsテーブルを生成します。

使用方法:
    # 全Run一括生成
    docker exec -it <container_id> python /app/scripts/generate_ports_batch.py

    # Dry-run
    docker exec -it <container_id> python /app/scripts/generate_ports_batch.py --dry-run

    # 特定Run除外
    docker exec -it <container_id> python /app/scripts/generate_ports_batch.py --exclude-run-id 1,2,3
"""

from define_db.database import SessionLocal
from define_db.models import Run, Process, Edge, Operation, Port, PortConnection
import argparse


def generate_fallback_ports_for_run(session, run_id: int, dry_run: bool = False) -> dict:
    """
    既存のEdgesテーブルから推測してPorts/PortConnectionsを生成

    Args:
        session: SQLAlchemy session
        run_id: Run ID
        dry_run: True の場合は実際には DB に書き込まない

    Returns:
        {"ports_created": int, "connections_created": int, "skipped": bool, "reason": str}
    """
    run = session.query(Run).filter(Run.id == run_id).first()
    if not run:
        return {"ports_created": 0, "connections_created": 0, "skipped": True, "reason": "Run not found"}

    # 既存のポートと接続をチェック
    existing_ports = session.query(Port).join(Process).filter(
        Process.run_id == run_id
    ).count()

    existing_connections = session.query(PortConnection).filter(
        PortConnection.run_id == run_id
    ).count()

    if existing_ports > 0 or existing_connections > 0:
        return {
            "ports_created": 0,
            "connections_created": 0,
            "skipped": True,
            "reason": f"Already has {existing_ports} ports and {existing_connections} connections"
        }

    # Edgesテーブルから接続情報を取得
    edges = session.query(Edge).filter(Edge.run_id == run_id).all()

    if not edges:
        return {
            "ports_created": 0,
            "connections_created": 0,
            "skipped": True,
            "reason": "No edges found"
        }

    # エッジから（プロセス間接続）を抽出
    process_connections = set()

    for edge in edges:
        from_op = session.query(Operation).filter(Operation.id == edge.from_id).first()
        to_op = session.query(Operation).filter(Operation.id == edge.to_id).first()

        if from_op and to_op:
            process_connections.add((from_op.process_id, to_op.process_id))

    if not process_connections:
        return {
            "ports_created": 0,
            "connections_created": 0,
            "skipped": True,
            "reason": "No valid process connections found"
        }

    ports_created = 0
    connections_created = 0

    # プロセスごとのポートカウンター
    process_output_count = {}
    process_input_count = {}

    for from_proc_id, to_proc_id in sorted(process_connections):
        from_process = session.query(Process).filter(Process.id == from_proc_id).first()
        to_process = session.query(Process).filter(Process.id == to_proc_id).first()

        if not from_process or not to_process:
            continue

        # 出力ポート作成
        output_count = process_output_count.get(from_proc_id, 0) + 1
        process_output_count[from_proc_id] = output_count

        output_port_name = f"output_{output_count}" if output_count > 1 else "output"

        output_port = Port(
            process_id=from_process.id,
            port_name=output_port_name,
            port_type="output",
            data_type="Unknown",
            position=output_count - 1,
            is_required=True,
            default_value=None,
            description=f"Generated output port to {to_process.name}"
        )

        # 入力ポート作成
        input_count = process_input_count.get(to_proc_id, 0) + 1
        process_input_count[to_proc_id] = input_count

        input_port_name = f"input_{input_count}" if input_count > 1 else "input"

        input_port = Port(
            process_id=to_process.id,
            port_name=input_port_name,
            port_type="input",
            data_type="Unknown",
            position=input_count - 1,
            is_required=True,
            default_value=None,
            description=f"Generated input port from {from_process.name}"
        )

        if not dry_run:
            session.add(output_port)
            session.add(input_port)
            session.flush()

            # PortConnection 作成
            connection = PortConnection(
                run_id=run_id,
                source_port_id=output_port.id,
                target_port_id=input_port.id
            )
            session.add(connection)

        ports_created += 2
        connections_created += 1

    if not dry_run:
        session.commit()

    return {
        "ports_created": ports_created,
        "connections_created": connections_created,
        "skipped": False,
        "reason": "Success"
    }


def batch_generate_ports(dry_run: bool = False, exclude_run_ids: list = None):
    """
    ポート情報がない全Runに対して一括生成

    Args:
        dry_run: True の場合は実際には DB に書き込まない
        exclude_run_ids: 除外するRun IDのリスト
    """
    exclude_run_ids = exclude_run_ids or []

    with SessionLocal() as session:
        # 削除されていないすべてのRunを取得
        runs = session.query(Run).filter(Run.deleted_at.is_(None)).all()

        total_runs = len(runs)
        processed = 0
        skipped = 0
        total_ports = 0
        total_connections = 0

        print(f"{'='*60}")
        print(f"Batch Port Generation - {'DRY RUN' if dry_run else 'LIVE MODE'}")
        print(f"{'='*60}")
        print(f"Found {total_runs} runs to process\n")

        for run in runs:
            if run.id in exclude_run_ids:
                print(f"Run {run.id:3d} ({run.file_name:20s}): ⏭️  Excluded by user")
                skipped += 1
                continue

            # 各Runを個別のセッションで処理（エラーの影響を最小化）
            try:
                with SessionLocal() as run_session:
                    result = generate_fallback_ports_for_run(run_session, run.id, dry_run)

                    if result["skipped"]:
                        print(f"Run {run.id:3d} ({run.file_name:20s}): ⏭️  {result['reason']}")
                        skipped += 1
                    else:
                        status = "[DRY RUN] Would create" if dry_run else "✅ Created"
                        print(f"Run {run.id:3d} ({run.file_name:20s}): {status} {result['ports_created']} ports, {result['connections_created']} connections")
                        processed += 1
                        total_ports += result["ports_created"]
                        total_connections += result["connections_created"]

            except Exception as e:
                print(f"Run {run.id:3d} ({run.file_name:20s}): ❌ Error: {e}")
                skipped += 1

        print(f"\n{'='*60}")
        print(f"Summary:")
        print(f"  Total Runs:        {total_runs}")
        print(f"  Processed:         {processed}")
        print(f"  Skipped:           {skipped}")
        if not dry_run:
            print(f"  Ports Created:     {total_ports}")
            print(f"  Connections:       {total_connections}")
        print(f"{'='*60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch generate fallback port data for all runs")
    parser.add_argument("--dry-run", action="store_true", help="Dry run (don't actually create data)")
    parser.add_argument("--exclude-run-id", type=str, help="Comma-separated list of run IDs to exclude (e.g., '1,2,3')")

    args = parser.parse_args()

    exclude_ids = []
    if args.exclude_run_id:
        exclude_ids = [int(x.strip()) for x in args.exclude_run_id.split(",")]

    batch_generate_ports(dry_run=args.dry_run, exclude_run_ids=exclude_ids)
