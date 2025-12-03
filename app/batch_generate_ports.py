#!/usr/bin/env python3
"""
全Run対象のフォールバックポート一括生成スクリプト（インライン版）
"""

from define_db.database import SessionLocal
from define_db.models import Run, Process, Edge, Operation, Port, PortConnection


def generate_fallback_ports_for_run(session, run_id: int) -> dict:
    """既存のEdgesテーブルから推測してPorts/PortConnectionsを生成"""
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
        return {"ports_created": 0, "connections_created": 0, "skipped": True, "reason": "No edges found"}

    # エッジから（プロセス間接続）を抽出
    process_connections = set()

    for edge in edges:
        from_op = session.query(Operation).filter(Operation.id == edge.from_id).first()
        to_op = session.query(Operation).filter(Operation.id == edge.to_id).first()

        if from_op and to_op:
            process_connections.add((from_op.process_id, to_op.process_id))

    if not process_connections:
        return {"ports_created": 0, "connections_created": 0, "skipped": True, "reason": "No valid process connections"}

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

    session.commit()

    return {"ports_created": ports_created, "connections_created": connections_created, "skipped": False, "reason": "Success"}


def batch_generate_ports():
    """ポート情報がない全Runに対して一括生成"""
    with SessionLocal() as session:
        # 削除されていないすべてのRunを取得
        runs = session.query(Run).filter(Run.deleted_at.is_(None)).all()

        total_runs = len(runs)
        processed = 0
        skipped = 0
        total_ports = 0
        total_connections = 0

        print(f"{'='*60}")
        print(f"Batch Port Generation - LIVE MODE")
        print(f"{'='*60}")
        print(f"Found {total_runs} runs to process\n")

        for run in runs:
            try:
                with SessionLocal() as run_session:
                    result = generate_fallback_ports_for_run(run_session, run.id)

                    if result["skipped"]:
                        print(f"Run {run.id:3d} ({run.file_name:20s}): ⏭️  {result['reason']}")
                        skipped += 1
                    else:
                        print(f"Run {run.id:3d} ({run.file_name:20s}): ✅ Created {result['ports_created']} ports, {result['connections_created']} connections")
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
        print(f"  Ports Created:     {total_ports}")
        print(f"  Connections:       {total_connections}")
        print(f"{'='*60}")


if __name__ == "__main__":
    batch_generate_ports()
