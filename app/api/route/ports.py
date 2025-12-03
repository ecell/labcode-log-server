"""
ポート関連API
"""
from fastapi import APIRouter, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from define_db.models import Run, Process, Port, PortConnection
from define_db.database import SessionLocal
from api.response_model import PortDetailResponse, PortConnectionResponse

router = APIRouter()


@router.get("/ports", tags=["ports"], response_model=List[PortDetailResponse])
def list_ports(
    process_id: int = Query(..., description="プロセスID"),
    port_type: Optional[str] = Query(None, description="input/output")
):
    """
    プロセスのポート一覧を取得

    Args:
        process_id: プロセスID
        port_type: ポート種別フィルタ (optional)

    Returns:
        List[PortDetailResponse]: ポート一覧
    """
    with SessionLocal() as session:
        query = session.query(Port).filter(Port.process_id == process_id)

        if port_type:
            if port_type not in ('input', 'output'):
                raise HTTPException(status_code=400, detail="Invalid port_type")
            query = query.filter(Port.port_type == port_type)

        ports = query.order_by(Port.position).all()

        return [PortDetailResponse.model_validate(p) for p in ports]


@router.get("/ports/{id}", tags=["ports"], response_model=PortDetailResponse)
def read_port(id: int):
    """
    ポート詳細を取得

    Args:
        id: ポートID

    Returns:
        PortDetailResponse: ポート詳細
    """
    with SessionLocal() as session:
        port = session.query(Port).filter(Port.id == id).first()
        if not port:
            raise HTTPException(status_code=404, detail="Port not found")

        return PortDetailResponse.model_validate(port)


@router.get("/runs/{run_id}/connections", tags=["runs"], response_model=List[PortConnectionResponse])
def get_connections(run_id: int):
    """
    Run全体のポート接続情報を取得(DAG描画用)

    Args:
        run_id: Run ID

    Returns:
        List[PortConnectionResponse]: 接続情報一覧
    """
    with SessionLocal() as session:
        # Run存在チェック
        run = session.query(Run).filter(Run.id == run_id).first()
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")

        # PortConnections取得
        result = []
        for conn in session.query(PortConnection).filter(PortConnection.run_id == run_id).all():
            source_port = session.query(Port).filter(Port.id == conn.source_port_id).first()
            target_port = session.query(Port).filter(Port.id == conn.target_port_id).first()

            if not source_port or not target_port:
                continue

            source_process = session.query(Process).filter(Process.id == source_port.process_id).first()
            target_process = session.query(Process).filter(Process.id == target_port.process_id).first()

            if not source_process or not target_process:
                continue

            result.append(PortConnectionResponse(
                connection_id=conn.id,
                run_id=conn.run_id,
                source_process_id=source_process.id,
                source_process_name=source_process.name,
                source_port_id=source_port.id,
                source_port_name=source_port.port_name,
                target_process_id=target_process.id,
                target_process_name=target_process.name,
                target_port_id=target_port.id,
                target_port_name=target_port.port_name
            ))

        return result
