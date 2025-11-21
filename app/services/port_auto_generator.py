"""
ポート自動生成サービス

Runが完了したときに自動的にポート情報を生成します。
YAMLファイルがアクセスできない場合の代替として、
Edgesテーブルから推測してポート情報を作成します。
"""

from sqlalchemy.orm import Session
from define_db.models import Run, Process, Edge, Operation, Port, PortConnection
from services.port_type_mapper import get_port_type_mapper
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def auto_generate_ports_for_run(session: Session, run_id: int) -> dict:
    """
    Run完了時に自動的にポート情報を生成

    1. YAMLファイルの存在確認
    2. YAMLがある場合: yaml_importer を使用
    3. YAMLがない場合: Edgesテーブルから推測して生成

    Args:
        session: SQLAlchemy session
        run_id: Run ID

    Returns:
        {"ports_created": int, "connections_created": int, "method": str}
    """
    run = session.query(Run).filter(Run.id == run_id).first()
    if not run:
        logger.warning(f"Run {run_id} not found")
        return {"ports_created": 0, "connections_created": 0, "method": "not_found"}

    # 既存のポートと接続をチェック（重複作成を防ぐ）
    existing_ports = session.query(Port).join(Process).filter(
        Process.run_id == run_id
    ).count()

    if existing_ports > 0:
        logger.info(f"Run {run_id} already has {existing_ports} ports, skipping auto-generation")
        return {"ports_created": 0, "connections_created": 0, "method": "already_exists"}

    # YAMLファイルの存在確認
    if run.storage_address and not run.storage_address.startswith("http"):
        protocol_path = Path(run.storage_address) / "protocol.yaml"
        manipulate_path = Path(run.storage_address) / "manipulate.yaml"

        if protocol_path.exists() and manipulate_path.exists():
            # YAMLファイルが存在する場合は yaml_importer を使用
            try:
                from services.yaml_importer import YAMLPortImporter
                importer = YAMLPortImporter(session)
                result = importer.import_from_run(run_id, run.storage_address)
                logger.info(f"Run {run_id}: Generated ports from YAML - {result['ports_created']} ports, {result['connections_created']} connections")
                return {
                    "ports_created": result['ports_created'],
                    "connections_created": result['connections_created'],
                    "method": "yaml"
                }
            except Exception as e:
                logger.error(f"Run {run_id}: Failed to import from YAML: {e}")
                # YAMLインポート失敗時はフォールバックへ

    # YAMLがない、またはインポート失敗時: Edgesテーブルから推測して生成
    result = _generate_ports_from_edges(session, run_id)
    if result["ports_created"] > 0:
        logger.info(f"Run {run_id}: Generated fallback ports - {result['ports_created']} ports, {result['connections_created']} connections")
    else:
        logger.warning(f"Run {run_id}: No ports generated - {result.get('reason', 'unknown')}")

    return result


def _generate_ports_from_edges(session: Session, run_id: int) -> dict:
    """
    Edgesテーブルから推測してポート情報を生成（フォールバック）

    Args:
        session: SQLAlchemy session
        run_id: Run ID

    Returns:
        {"ports_created": int, "connections_created": int, "method": str, "reason": str}
    """
    # Edgesテーブルから接続情報を取得
    edges = session.query(Edge).filter(Edge.run_id == run_id).all()

    if not edges:
        return {
            "ports_created": 0,
            "connections_created": 0,
            "method": "fallback",
            "reason": "no_edges"
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
            "method": "fallback",
            "reason": "no_process_connections"
        }

    ports_created = 0
    connections_created = 0

    # ポート型マッパーを取得
    type_mapper = get_port_type_mapper()

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

        # ポート型を取得（manipulate.yamlから）
        output_data_type = "Unknown"
        if from_process.process_type:
            # まず、定義されたポート名（例: "value", "out1"）で型を探す
            all_ports = type_mapper.get_all_ports_for_process(from_process.process_type)
            if all_ports and all_ports.get('output'):
                output_ports_def = all_ports['output']
                # 接続インデックスに対応するポートの型を取得
                if output_count - 1 < len(output_ports_def):
                    port_def = output_ports_def[output_count - 1]
                    output_data_type = port_def.get('type', 'Unknown')
                    # 実際のポート名を使用（YAML定義のid）
                    output_port_name = port_def.get('id', output_port_name)

        output_port = Port(
            process_id=from_process.id,
            port_name=output_port_name,
            port_type="output",
            data_type=output_data_type,
            position=output_count - 1,
            is_required=True,
            default_value=None,
            description=f"Auto-generated output port to {to_process.name}"
        )

        # 入力ポート作成
        input_count = process_input_count.get(to_proc_id, 0) + 1
        process_input_count[to_proc_id] = input_count

        input_port_name = f"input_{input_count}" if input_count > 1 else "input"

        # ポート型を取得（manipulate.yamlから）
        input_data_type = "Unknown"
        if to_process.process_type:
            all_ports = type_mapper.get_all_ports_for_process(to_process.process_type)
            if all_ports and all_ports.get('input'):
                input_ports_def = all_ports['input']
                # 接続インデックスに対応するポートの型を取得
                if input_count - 1 < len(input_ports_def):
                    port_def = input_ports_def[input_count - 1]
                    input_data_type = port_def.get('type', 'Unknown')
                    # 実際のポート名を使用（YAML定義のid）
                    input_port_name = port_def.get('id', input_port_name)

        input_port = Port(
            process_id=to_process.id,
            port_name=input_port_name,
            port_type="input",
            data_type=input_data_type,
            position=input_count - 1,
            is_required=True,
            default_value=None,
            description=f"Auto-generated input port from {from_process.name}"
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

    # コミットは呼び出し側で実行
    return {
        "ports_created": ports_created,
        "connections_created": connections_created,
        "method": "fallback",
        "reason": "success"
    }
