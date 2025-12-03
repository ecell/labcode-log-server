#!/usr/bin/env python3
"""
テストデータ: Process 7のポート情報をDBに挿入するスクリプト

Usage:
    docker exec -it <container_id> python /app/playground_nova/insert_test_ports.py

または、コンテナ外から:
    docker cp playground_nova/insert_test_ports.py <container_id>:/app/
    docker exec -it <container_id> python /app/insert_test_ports.py
"""

import sys
from pathlib import Path

# Import path adjustment for Docker environment
sys.path.insert(0, '/app')

from define_db.database import SessionLocal
from define_db.models import Process, Port, PortConnection

def insert_test_ports_for_process_7():
    """
    Process 7 (serve_plate1) のテストポートデータを挿入

    仮定するプロセス構成:
    - Process 7: serve_plate1
    - Input ports: plate (Plate96), config (PlateConfig)
    - Output ports: served_plate (Plate96)
    """
    with SessionLocal() as session:
        # Process 7が存在するか確認
        process = session.query(Process).filter(Process.id == 7).first()
        if not process:
            print("❌ Process 7 not found")
            return

        print(f"✅ Found Process 7: {process.name} (run_id={process.run_id})")

        # 既存のポートがあれば削除（冪等性確保）
        existing_ports = session.query(Port).filter(Port.process_id == 7).all()
        if existing_ports:
            print(f"🗑️  Deleting {len(existing_ports)} existing ports for process 7")
            for port in existing_ports:
                session.delete(port)
            session.commit()

        # 入力ポート作成
        input_ports_data = [
            {
                'port_name': 'plate',
                'data_type': 'Plate96',
                'position': 0,
                'description': 'Input plate to serve'
            },
            {
                'port_name': 'config',
                'data_type': 'PlateConfig',
                'position': 1,
                'description': 'Serving configuration'
            }
        ]

        created_input_ports = []
        for idx, port_data in enumerate(input_ports_data):
            port = Port(
                process_id=7,
                port_name=port_data['port_name'],
                port_type='input',
                data_type=port_data['data_type'],
                position=port_data['position'],
                is_required=True,
                default_value=None,
                description=port_data['description']
            )
            session.add(port)
            created_input_ports.append(port)
            print(f"  ➕ Input port: {port_data['port_name']} ({port_data['data_type']})")

        # 出力ポート作成
        output_ports_data = [
            {
                'port_name': 'served_plate',
                'data_type': 'Plate96',
                'position': 0,
                'description': 'Served plate output'
            }
        ]

        created_output_ports = []
        for idx, port_data in enumerate(output_ports_data):
            port = Port(
                process_id=7,
                port_name=port_data['port_name'],
                port_type='output',
                data_type=port_data['data_type'],
                position=port_data['position'],
                is_required=True,
                default_value=None,
                description=port_data['description']
            )
            session.add(port)
            created_output_ports.append(port)
            print(f"  ➕ Output port: {port_data['port_name']} ({port_data['data_type']})")

        # コミット
        session.commit()

        print(f"\n✅ Successfully created {len(created_input_ports)} input ports and {len(created_output_ports)} output ports for Process 7")
        print(f"\n🔍 Test by running:")
        print(f"   curl -s http://localhost:8000/api/processes/7 | jq .ports")


if __name__ == "__main__":
    insert_test_ports_for_process_7()
