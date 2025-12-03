from define_db.models import Operation, Process, Run
from define_db.database import SessionLocal
from api.response_model import OperationResponse
from fastapi import APIRouter
from fastapi import Form
from fastapi import HTTPException
from fastapi import Query
from typing import Optional
from datetime import datetime

router = APIRouter()


@router.get("/operations", tags=["operations"])
def get_all_operations(
    user_id: Optional[int] = Query(None, description="Filter by user_id"),
    run_id: Optional[int] = Query(None, description="Filter by run_id"),
    process_id: Optional[int] = Query(None, description="Filter by process_id"),
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(1000, description="Limit number of results", ge=1, le=10000),
    offset: int = Query(0, description="Offset for pagination", ge=0)
):
    """
    全オペレーション一覧を取得する。
    Operation → Process → Run のJOINクエリを実行し、run_idを含めたレスポンスを返す。

    Parameters:
    - user_id: ユーザーIDでフィルタリング(オプション)
    - run_id: ランIDでフィルタリング(オプション)
    - process_id: プロセスIDでフィルタリング(オプション)
    - status: ステータスでフィルタリング(オプション)
    - limit: 取得件数制限(デフォルト: 1000)
    - offset: オフセット(デフォルト: 0)
    """
    with SessionLocal() as session:
        # Operation → Process → Run のJOINクエリ
        query = session.query(
            Operation,
            Process.run_id.label('run_id')
        ).join(
            Process, Operation.process_id == Process.id
        ).join(
            Run, Process.run_id == Run.id
        )

        # フィルタ適用
        if user_id is not None:
            query = query.filter(Run.user_id == user_id)
        if run_id is not None:
            query = query.filter(Process.run_id == run_id)
        if process_id is not None:
            query = query.filter(Operation.process_id == process_id)
        if status is not None:
            query = query.filter(Operation.status == status)

        # 論理削除されたRunを除外
        query = query.filter(Run.deleted_at.is_(None))

        # ページネーション
        query = query.limit(limit).offset(offset)

        # 実行
        results = query.all()

        # レスポンス構築
        operations = []
        for operation, run_id in results:
            op_dict = OperationResponse.model_validate(operation).model_dump()
            op_dict['run_id'] = run_id
            operations.append(op_dict)

        return operations


@router.post("/operations/", tags=["operations"], response_model=OperationResponse)
def create(
        process_id: int = Form(),
        name: str = Form(),
        parent_id: Optional[int] = Form(default=None),
        started_at: Optional[str] = Form(default=None),
        finished_at: Optional[str] = Form(default=None),
        status: str = Form(default="not started"),
        storage_address: str = Form(),
        is_transport: bool = Form(),
        is_data: bool = Form()

):
    with SessionLocal() as session:
        # Check process existence
        process = session.query(Process).filter(Process.id == process_id).first()
        if not process:
            raise HTTPException(status_code=400, detail=f"Process with id {process_id} not found")
        # Check parent existence (if specified)
        if parent_id:
            parent = session.query(Operation).filter(Operation.id == parent_id).first()
            if not parent:
                raise HTTPException(status_code=400, detail=f"Parent with id {parent_id} not found")
        operation_to_add = Operation(
            process_id=process_id,
            name=name,
            parent_id=parent_id,
            started_at=started_at,
            finished_at=finished_at,
            status=status,
            storage_address=storage_address,
            is_transport=is_transport,
            is_data=is_data
        )
        session.add_all([operation_to_add])
        session.commit()
        session.refresh(operation_to_add)
        return OperationResponse.model_validate(operation_to_add)


@router.get("/operations/{id}", tags=["operations"], response_model=OperationResponse)
def read(id: int):
    with SessionLocal() as session:
        operation = session.query(Operation).filter(Operation.id == id).first()
        if not operation:
            raise HTTPException(status_code=404, detail="Operation not found")
        return OperationResponse.model_validate(operation)


@router.put("/operations/{id}", tags=["operations"], response_model=OperationResponse)
def update(
        id: int,
        process_id: int = Form(),
        name: str = Form(),
        parent_id: Optional[int] = Form(default=None),
        started_at: Optional[str] = Form(default=None),
        finished_at: Optional[str] = Form(default=None),
        status: str = Form(default="not started"),
        storage_address: str = Form(),
        is_transport: bool = Form(),
        is_data: bool = Form()
):
    with SessionLocal() as session:
        operation = session.query(Operation).filter(Operation.id == id).first()
        if not operation:
            raise HTTPException(status_code=404, detail="Operation not found")
        # Check process existence
        process = session.query(Process).filter(Process.id == process_id).first()
        if not process:
            raise HTTPException(status_code=400, detail=f"Process with id {process_id} not found")
        # Check parent existence (if specified)
        if parent_id:
            parent = session.query(Operation).filter(Operation.id == parent_id).first()
            if not parent:
                raise HTTPException(status_code=400, detail=f"Parent with id {parent_id} not found")
        operation.process_id = process_id
        operation.name = name
        operation.parent_id = parent_id
        operation.started_at = started_at
        operation.finished_at = finished_at
        operation.status = status
        operation.storage_address = storage_address
        operation.is_transport = is_transport
        operation.is_data = is_data
        session.commit()
        session.refresh(operation)
        return OperationResponse.model_validate(operation)


@router.patch("/operations/{id}", tags=["operations"], response_model=OperationResponse)
def patch(id: int, attribute: str = Form(), new_value: str = Form()):
    with SessionLocal() as session:
        operation = session.query(Operation).filter(Operation.id == id).first()
        if not operation:
            raise HTTPException(status_code=404, detail="Operation not found")
        match attribute:
            case "process_id":
                process = session.query(Process).filter(Process.id == new_value).first()
                if not process:
                    raise HTTPException(status_code=400, detail=f"Process with id {new_value} not found")
                operation.process_id = new_value
            case "name":
                operation.name = new_value
            case "parent_id":
                parent = session.query(Operation).filter(Operation.id == new_value).first()
                if not parent:
                    raise HTTPException(status_code=400, detail=f"Parent with id {new_value} not found")
                operation.parent_id = new_value
            case "started_at":
                new_datetime = datetime.fromisoformat(new_value)
                operation.started_at = new_datetime
            case "finished_at":
                new_datetime = datetime.fromisoformat(new_value)
                operation.finished_at = new_datetime
            case "status":
                operation.status = new_value
            case "storage_address":
                operation.storage_address = new_value
            case "log":
                operation.log = new_value
            case _:
                raise HTTPException(status_code=400, detail="Invalid attribute")
        session.commit()
        session.refresh(operation)
        return OperationResponse.model_validate(operation)


@router.delete("/operations/{id}", tags=["operations"])
def delete(id: int):
    with SessionLocal() as session:
        operation = session.query(Operation).filter(Operation.id == id).first()
        if not operation:
            raise HTTPException(status_code=404, detail="Operation not found")
        session.delete(operation)
        session.commit()
        return {"message": "Operation deleted successfully"}
