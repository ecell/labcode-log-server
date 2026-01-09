"""ProcessOperation中間テーブルのAPI

ProcessとOperationのN:M関係を管理するAPIエンドポイント。
"""

from fastapi import APIRouter, HTTPException, Query, Body
from typing import Optional, List
from pydantic import BaseModel
from define_db.models import ProcessOperation, Process, Operation
from define_db.database import SessionLocal

router = APIRouter()


class ProcessOperationCreate(BaseModel):
    """ProcessOperation作成リクエスト"""
    process_id: int
    operation_id: int


class ProcessOperationResponse(BaseModel):
    """ProcessOperationレスポンス"""
    id: int
    process_id: int
    operation_id: int
    created_at: Optional[str] = None

    class Config:
        from_attributes = True


@router.get("/process-operations", tags=["process-operations"])
def get_process_operations(
    process_id: Optional[int] = Query(None, description="Filter by process_id"),
    operation_id: Optional[int] = Query(None, description="Filter by operation_id"),
    limit: int = Query(1000, description="Limit number of results", ge=1, le=10000),
    offset: int = Query(0, description="Offset for pagination", ge=0)
) -> List[ProcessOperationResponse]:
    """
    ProcessOperation一覧を取得する。

    Parameters:
    - process_id: プロセスIDでフィルタリング(オプション)
    - operation_id: オペレーションIDでフィルタリング(オプション)
    - limit: 取得件数制限(デフォルト: 1000)
    - offset: オフセット(デフォルト: 0)
    """
    with SessionLocal() as session:
        query = session.query(ProcessOperation)

        if process_id is not None:
            query = query.filter(ProcessOperation.process_id == process_id)
        if operation_id is not None:
            query = query.filter(ProcessOperation.operation_id == operation_id)

        query = query.limit(limit).offset(offset)
        results = query.all()

        return [
            ProcessOperationResponse(
                id=po.id,
                process_id=po.process_id,
                operation_id=po.operation_id,
                created_at=po.created_at.isoformat() if po.created_at else None
            )
            for po in results
        ]


@router.post("/process-operations", tags=["process-operations"])
def create_process_operation(
    data: ProcessOperationCreate = Body(...)
) -> ProcessOperationResponse:
    """
    ProcessOperationを作成する。

    Parameters:
    - process_id: プロセスID
    - operation_id: オペレーションID
    """
    with SessionLocal() as session:
        # プロセス存在確認
        process = session.query(Process).filter(Process.id == data.process_id).first()
        if not process:
            raise HTTPException(
                status_code=404,
                detail=f"Process with id {data.process_id} not found"
            )

        # オペレーション存在確認
        operation = session.query(Operation).filter(Operation.id == data.operation_id).first()
        if not operation:
            raise HTTPException(
                status_code=404,
                detail=f"Operation with id {data.operation_id} not found"
            )

        # 重複チェック
        existing = session.query(ProcessOperation).filter(
            ProcessOperation.process_id == data.process_id,
            ProcessOperation.operation_id == data.operation_id
        ).first()
        if existing:
            raise HTTPException(
                status_code=409,
                detail="ProcessOperation already exists"
            )

        # 作成
        po = ProcessOperation(
            process_id=data.process_id,
            operation_id=data.operation_id
        )
        session.add(po)
        session.commit()
        session.refresh(po)

        return ProcessOperationResponse(
            id=po.id,
            process_id=po.process_id,
            operation_id=po.operation_id,
            created_at=po.created_at.isoformat() if po.created_at else None
        )


@router.get("/process-operations/{id}", tags=["process-operations"])
def get_process_operation(id: int) -> ProcessOperationResponse:
    """
    ProcessOperationを取得する。

    Parameters:
    - id: ProcessOperation ID
    """
    with SessionLocal() as session:
        po = session.query(ProcessOperation).filter(ProcessOperation.id == id).first()
        if not po:
            raise HTTPException(status_code=404, detail="ProcessOperation not found")

        return ProcessOperationResponse(
            id=po.id,
            process_id=po.process_id,
            operation_id=po.operation_id,
            created_at=po.created_at.isoformat() if po.created_at else None
        )


@router.delete("/process-operations/{id}", tags=["process-operations"])
def delete_process_operation(id: int):
    """
    ProcessOperationを削除する。

    Parameters:
    - id: ProcessOperation ID
    """
    with SessionLocal() as session:
        po = session.query(ProcessOperation).filter(ProcessOperation.id == id).first()
        if not po:
            raise HTTPException(status_code=404, detail="ProcessOperation not found")

        session.delete(po)
        session.commit()

        return {"message": f"ProcessOperation {id} deleted successfully"}
