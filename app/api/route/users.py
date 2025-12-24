from define_db.models import User, Run, Project
from define_db.database import SessionLocal
from api.response_model import UserResponse, RunResponseWithProjectName, ProjectResponse
from services.hal import batch_infer_storage_modes
from fastapi import Form, Query
from fastapi import APIRouter
from fastapi import HTTPException
from sqlalchemy.orm import joinedload, selectinload
from typing import List

router = APIRouter()


# ============================================================
# Admin API: ユーザー一覧取得
# ============================================================

@router.get("/users/list", tags=["users"], response_model=List[UserResponse])
def list_all(
    limit: int = Query(default=100, ge=1, le=1000, description="Maximum number of users to return"),
    offset: int = Query(default=0, ge=0, description="Number of users to skip")
):
    """
    全ユーザー一覧を取得

    管理画面のユーザー一覧表示で使用。
    ページネーション対応。
    """
    with SessionLocal() as session:
        users = session.query(User).offset(offset).limit(limit).all()
        return [UserResponse.model_validate(u) for u in users]


# ============================================================
# Admin API: ユーザーのプロジェクト一覧取得
# ============================================================

@router.get("/users/{id}/projects", tags=["users"], response_model=List[ProjectResponse])
def read_user_projects(id: int):
    """
    ユーザーのプロジェクト一覧を取得

    指定されたユーザーが所有するプロジェクトの一覧を返す。
    """
    with SessionLocal() as session:
        user = session.query(User).filter(User.id == id).first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        projects = session.query(Project).filter(Project.user_id == id).all()
        return [ProjectResponse.model_validate(p) for p in projects]


@router.post("/users/", tags=["users"], response_model=UserResponse)
def create(email: str = Form()) -> User:
    with SessionLocal() as session:
        # ユーザーの存在確認
        user = session.query(User).filter(User.email == email).first()
        if user:
            raise HTTPException(status_code=400, detail="Email already registered")
        user_to_add = User(email=email)
        session.add_all([user_to_add])
        session.commit()
        session.refresh(user_to_add)
        return UserResponse.model_validate(user_to_add)


@router.get("/users/{id}", tags=["users"], response_model=UserResponse)
def read(id: int):
    with SessionLocal() as session:
        user = session.query(User).filter(User.id == id).first()
        if user:
            return UserResponse.model_validate(user)
        else:
            raise HTTPException(status_code=404, detail="User not found")


@router.get("/users/", tags=["users"], response_model=UserResponse)
def read_by_email(email: str):
    with SessionLocal() as session:
        user = session.query(User).filter(User.email == email).first()
        if user:
            return UserResponse.model_validate(user)
        else:
            raise HTTPException(status_code=404, detail="User not found")


@router.get("/users/{id}/runs", tags=["users"], response_model=List[RunResponseWithProjectName])
def read_runs(id: int, include_hidden: bool = False):
    with SessionLocal() as session:
        user = session.query(User).filter(User.id == id).first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        # Build query with deleted_at filter
        query = session.query(Run).options(selectinload(Run.project)).filter(
            Run.user_id == id,
            Run.deleted_at.is_(None)
        )

        # Filter by display_visible unless include_hidden is True
        if not include_hidden:
            query = query.filter(Run.display_visible == True)

        runs = query.all()

        # バッチ最適化: 未キャッシュのRunのstorage_modeを一括推論・永続化
        # 1回のS3リクエスト + 1回のDBクエリで全Run判定（N回→2回に削減）
        batch_infer_storage_modes(session, runs)

        for run in runs:
            run.project_name = run.project.name
        return runs


@router.put("/users/{id}", tags=["users"], response_model=UserResponse)
def update(id: int, email: str = Form()):
    with SessionLocal() as session:
        user = session.query(User).filter(User.id == id).first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        user.email = email
        session.commit()
        session.refresh(user)
        return UserResponse.model_validate(user)


@router.patch("/users/{id}", tags=["users"], response_model=UserResponse)
def patch(id: int, attribute: str = Form(), new_value: str = Form()):
    with SessionLocal() as session:
        user = session.query(User).filter(User.id == id).first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        match attribute:
            case "email":
                user.email = new_value
            case _:
                raise HTTPException(status_code=400, detail="Invalid attribute")
        session.commit()
        session.refresh(user)
        return UserResponse.model_validate(user)


@router.delete("/users/{id}", tags=["users"])
def delete(id: int):
    with SessionLocal() as session:
        user = session.query(User).filter(User.id == id).first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        session.delete(user)
        session.commit()
        return {"message": "User deleted successfully"}
