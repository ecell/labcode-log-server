"""
YAMLファイルからポート情報をDBにインポートするサービス
"""
from pathlib import Path
from typing import Dict, List
import yaml
from sqlalchemy.orm import Session
from define_db.models import Process, Run, Port, PortConnection
from define_db.database import SessionLocal


class YAMLPortImporter:
    """YAMLファイルからポート情報をインポート"""

    def __init__(self, session: Session):
        self.session = session

    def import_from_run(self, run_id: int, storage_address: str) -> Dict[str, int]:
        """
        Runのポート情報をYAMLから一括インポート

        Args:
            run_id: Run ID
            storage_address: YAMLファイルのあるディレクトリパス

        Returns:
            {"ports_created": 10, "connections_created": 5}

        Raises:
            FileNotFoundError: YAML不存在
            yaml.YAMLError: YAML解析エラー
        """
        # YAMLファイル読み込み
        protocol_path = Path(storage_address) / "protocol.yaml"
        manipulate_path = Path(storage_address) / "manipulate.yaml"

        if not protocol_path.exists() or not manipulate_path.exists():
            raise FileNotFoundError(f"YAML files not found at {storage_address}")

        with open(protocol_path, 'r', encoding='utf-8') as f:
            protocol_data = yaml.safe_load(f)

        with open(manipulate_path, 'r', encoding='utf-8') as f:
            manipulate_data = yaml.safe_load(f)

        # このRunのすべてのProcessを取得
        processes = self.session.query(Process).filter(
            Process.run_id == run_id
        ).all()

        ports_created = 0
        connections_created = 0

        # 各ProcessのPorts作成
        for process in processes:
            created = self._import_ports_for_process(
                process, protocol_data, manipulate_data
            )
            ports_created += created

        # Connections作成
        created = self._import_connections(
            run_id, processes, protocol_data
        )
        connections_created += created

        self.session.commit()

        return {
            "ports_created": ports_created,
            "connections_created": connections_created
        }

    def _import_ports_for_process(
        self,
        process: Process,
        protocol_data: Dict,
        manipulate_data: List[Dict]
    ) -> int:
        """1つのProcessのPorts作成"""
        # protocol.yamlからプロセスタイプを取得
        process_type = None
        for op in protocol_data.get('operations', []):
            if op.get('id') == process.name:
                process_type = op.get('type')
                break

        if not process_type:
            print(f"Warning: Process type not found for {process.name}")
            return 0

        # ★NEW: ProcessレコードにもProcess typeを保存
        if not process.process_type:
            process.process_type = process_type

        # manipulate.yamlからポート定義を取得
        process_def = None
        for pdef in manipulate_data:
            if pdef.get('name') == process_type:
                process_def = pdef
                break

        if not process_def:
            print(f"Warning: Process definition not found for type {process_type}")
            return 0

        created_count = 0

        # 入力ポート作成
        for idx, port_def in enumerate(process_def.get('input', [])):
            port = Port(
                process_id=process.id,
                port_name=port_def.get('id'),
                port_type='input',
                data_type=port_def.get('type'),
                position=idx,
                is_required=True,
                default_value=yaml.dump(port_def.get('default')) if port_def.get('default') else None,
                description=port_def.get('description')
            )
            self.session.add(port)
            created_count += 1

        # 出力ポート作成
        for idx, port_def in enumerate(process_def.get('output', [])):
            port = Port(
                process_id=process.id,
                port_name=port_def.get('id'),
                port_type='output',
                data_type=port_def.get('type'),
                position=idx,
                is_required=True,
                default_value=None,
                description=port_def.get('description')
            )
            self.session.add(port)
            created_count += 1

        return created_count

    def _import_connections(
        self,
        run_id: int,
        processes: List[Process],
        protocol_data: Dict
    ) -> int:
        """PortConnection作成"""
        connections = protocol_data.get('connections', [])
        created_count = 0

        # プロセス名→Processオブジェクトのマップ
        process_map = {p.name: p for p in processes}

        for conn_def in connections:
            # input側が出力元、output側が入力先
            input_info = conn_def.get('input', [])  # [process_name, port_name]
            output_info = conn_def.get('output', [])  # [process_name, port_name]

            if len(input_info) < 2 or len(output_info) < 2:
                continue

            source_process_name = input_info[0]
            source_port_name = input_info[1]
            target_process_name = output_info[0]
            target_port_name = output_info[1]

            # プロセス取得
            source_process = process_map.get(source_process_name)
            target_process = process_map.get(target_process_name)

            if not source_process or not target_process:
                continue

            # ポート取得
            source_port = self.session.query(Port).filter(
                Port.process_id == source_process.id,
                Port.port_name == source_port_name,
                Port.port_type == 'output'
            ).first()

            target_port = self.session.query(Port).filter(
                Port.process_id == target_process.id,
                Port.port_name == target_port_name,
                Port.port_type == 'input'
            ).first()

            if not source_port or not target_port:
                continue

            # 接続作成
            connection = PortConnection(
                run_id=run_id,
                source_port_id=source_port.id,
                target_port_id=target_port.id
            )
            self.session.add(connection)
            created_count += 1

        return created_count


def import_ports_for_all_runs():
    """全Runのポート情報をインポート (既存データ移行用)"""
    with SessionLocal() as session:
        runs = session.query(Run).filter(Run.deleted_at.is_(None)).all()
        importer = YAMLPortImporter(session)

        total_ports = 0
        total_connections = 0
        success_count = 0
        failure_count = 0

        print(f"Found {len(runs)} runs to migrate")

        for run in runs:
            # storage_addressがGoogle Drive URLの場合はスキップ
            if run.storage_address.startswith('http'):
                print(f"Run {run.id}: Skipped (remote URL): {run.storage_address}")
                continue

            try:
                print(f"Processing Run {run.id}...")
                result = importer.import_from_run(run.id, run.storage_address)
                print(f"  Created {result['ports_created']} ports, {result['connections_created']} connections")
                total_ports += result['ports_created']
                total_connections += result['connections_created']
                success_count += 1
            except Exception as e:
                print(f"  Error: {e}")
                failure_count += 1

        print(f"\n=== Migration Summary ===")
        print(f"Total runs: {len(runs)}")
        print(f"Success: {success_count}")
        print(f"Failure: {failure_count}")
        print(f"Total ports imported: {total_ports}")
        print(f"Total connections imported: {total_connections}")


if __name__ == "__main__":
    # 既存データ移行実行
    import_ports_for_all_runs()
