"""
ポート関連のビジネスロジック
"""
from sqlalchemy.orm import Session
from define_db.models import Process, Run, Port, PortConnection
from fastapi import HTTPException
from typing import Dict, List, Optional


def create_ports_for_process(
    session: Session,
    process_id: int,
    ports_definition: Dict
) -> int:
    """
    Processに対応するPortsを作成

    Args:
        session: SQLAlchemyセッション
        process_id: プロセスID
        ports_definition: ポート定義
            {
                "input": [{"name": "in1", "data_type": "Plate96", ...}],
                "output": [{"name": "value", "data_type": "Plate96", ...}]
            }

    Returns:
        int: 作成されたPort数

    Raises:
        HTTPException: Process不存在時
    """
    # Process存在確認
    process = session.query(Process).filter(Process.id == process_id).first()
    if not process:
        raise HTTPException(status_code=404, detail=f"Process {process_id} not found")

    created_count = 0

    # 入力ポート作成
    for idx, port_def in enumerate(ports_definition.get('input', [])):
        port = Port(
            process_id=process_id,
            port_name=port_def['name'],
            port_type='input',
            data_type=port_def['data_type'],
            position=idx,
            is_required=port_def.get('is_required', True),
            default_value=port_def.get('default_value'),
            description=port_def.get('description')
        )
        session.add(port)
        created_count += 1

    # 出力ポート作成
    for idx, port_def in enumerate(ports_definition.get('output', [])):
        port = Port(
            process_id=process_id,
            port_name=port_def['name'],
            port_type='output',
            data_type=port_def['data_type'],
            position=idx,
            is_required=port_def.get('is_required', True),
            default_value=port_def.get('default_value'),
            description=port_def.get('description')
        )
        session.add(port)
        created_count += 1

    session.commit()
    return created_count


def create_port_connections(
    session: Session,
    run_id: int,
    connections: List[Dict]
) -> int:
    """
    ポート間接続を作成

    Args:
        session: SQLAlchemyセッション
        run_id: Run ID
        connections: 接続定義リスト
            [
                {
                    "source_process": "serve_plate1",
                    "source_port": "value",
                    "target_process": "dispense_liquid1",
                    "target_port": "in1"
                },
                ...
            ]

    Returns:
        int: 作成された接続数

    Raises:
        HTTPException: Run不存在、Port不存在時
    """
    # Run存在確認
    run = session.query(Run).filter(Run.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    # このRunの全Process取得 (名前→IDマップ作成)
    processes = session.query(Process).filter(Process.run_id == run_id).all()
    process_map = {p.name: p.id for p in processes}

    created_count = 0

    for conn_def in connections:
        source_process_name = conn_def['source_process']
        source_port_name = conn_def['source_port']
        target_process_name = conn_def['target_process']
        target_port_name = conn_def['target_port']

        # Process ID取得
        source_process_id = process_map.get(source_process_name)
        target_process_id = process_map.get(target_process_name)

        if not source_process_id or not target_process_id:
            print(f"Warning: Process not found for connection {conn_def}")
            continue

        # Port ID取得
        source_port = session.query(Port).filter(
            Port.process_id == source_process_id,
            Port.port_name == source_port_name,
            Port.port_type == 'output'
        ).first()

        target_port = session.query(Port).filter(
            Port.process_id == target_process_id,
            Port.port_name == target_port_name,
            Port.port_type == 'input'
        ).first()

        if not source_port or not target_port:
            print(f"Warning: Port not found for connection {conn_def}")
            continue

        # PortConnection作成
        connection = PortConnection(
            run_id=run_id,
            source_port_id=source_port.id,
            target_port_id=target_port.id
        )
        session.add(connection)
        created_count += 1

    session.commit()
    return created_count


def get_ports_by_process(session: Session, process_id: int) -> List[Port]:
    """指定Processの全Ports取得"""
    return session.query(Port).filter(
        Port.process_id == process_id
    ).order_by(Port.position).all()


def get_port_connections_by_run(session: Session, run_id: int) -> List[PortConnection]:
    """指定Runの全PortConnections取得"""
    return session.query(PortConnection).filter(
        PortConnection.run_id == run_id
    ).all()
