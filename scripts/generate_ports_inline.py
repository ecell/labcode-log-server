#!/usr/bin/env python3
"""
Inline version of generate_fallback_ports.py that runs from /app directory
"""

from define_db.database import SessionLocal
from define_db.models import Run, Process, Edge, Operation, Port, PortConnection

def generate_fallback_ports_for_run(run_id: int):
    """Generate fallback ports from edges"""
    with SessionLocal() as session:
        run = session.query(Run).filter(Run.id == run_id).first()
        if not run:
            print(f"❌ Run {run_id} not found.")
            return {"ports_created": 0, "connections_created": 0}

        print(f"Processing Run {run.id}: {run.file_name}")

        # 既存のポートと接続をチェック
        existing_ports = session.query(Port).join(Process).filter(
            Process.run_id == run_id
        ).count()

        existing_connections = session.query(PortConnection).filter(
            PortConnection.run_id == run_id
        ).count()

        if existing_ports > 0 or existing_connections > 0:
            print(f"⚠️  Run {run_id} already has {existing_ports} ports and {existing_connections} connections.")
            print(f"   Skipping to avoid duplication.")
            return {"ports_created": 0, "connections_created": 0}

        # Edgesテーブルから接続情報を取得
        edges = session.query(Edge).filter(Edge.run_id == run_id).all()

        if not edges:
            print(f"⚠️  No edges found for Run {run_id}. Nothing to generate.")
            return {"ports_created": 0, "connections_created": 0}

        print(f"Found {len(edges)} edges for Run {run_id}")

        # エッジから（プロセス間接続）を抽出
        process_connections = set()

        for edge in edges:
            from_op = session.query(Operation).filter(Operation.id == edge.from_id).first()
            to_op = session.query(Operation).filter(Operation.id == edge.to_id).first()

            if from_op and to_op:
                process_connections.add((from_op.process_id, to_op.process_id))

        print(f"Identified {len(process_connections)} unique process-to-process connections")

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

            print(f"  ✅ Created: {from_process.name}.{output_port_name} -> {to_process.name}.{input_port_name}")

        session.commit()
        print(f"\n✅ Successfully created {ports_created} ports and {connections_created} connections for Run {run_id}")

        return {"ports_created": ports_created, "connections_created": connections_created}

if __name__ == "__main__":
    generate_fallback_ports_for_run(15)
