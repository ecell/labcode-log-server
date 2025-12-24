from define_db.models import Project, User
from define_db.database import SessionLocal
from api.response_model import ProjectResponse, ProjectResponseWithOwner
from fastapi import APIRouter, Query
from fastapi import Form
from fastapi import HTTPException
from sqlalchemy.orm import joinedload
from typing import List
import datetime as dt

router = APIRouter()


# ============================================================
# Admin API: プロジェクト一覧取得（オーナー情報含む）
# ============================================================

@router.get("/projects/list", tags=["projects"], response_model=List[ProjectResponseWithOwner])
def list_all(
    limit: int = Query(default=100, ge=1, le=1000, description="Maximum number of projects to return"),
    offset: int = Query(default=0, ge=0, description="Number of projects to skip")
):
    """
    全プロジェクト一覧を取得（オーナー情報含む）

    管理画面のプロジェクト一覧表示で使用。
    オーナーのメールアドレスを含む。
    ページネーション対応。
    """
    with SessionLocal() as session:
        projects = session.query(Project).options(
            joinedload(Project.user)
        ).offset(offset).limit(limit).all()

        result = []
        for p in projects:
            resp = ProjectResponseWithOwner(
                id=p.id,
                name=p.name,
                user_id=p.user_id,
                owner_email=p.user.email if p.user else None,
                created_at=p.created_at,
                updated_at=p.updated_at
            )
            result.append(resp)
        return result


@router.post("/projects/", tags=["projects"], response_model=ProjectResponse)
def create(name: str = Form(), user_id: int = Form()):
    with SessionLocal() as session:
        # ユーザーの存在確認
        user = session.query(User).filter(User.id == user_id).first()
        if not user:
            raise HTTPException(status_code=400, detail=f"User with id {user_id} not found")
        project_to_add = Project(
            name=name,
            user_id=user_id,
            created_at=dt.datetime.now(),
            updated_at=dt.datetime.now()
        )
        session.add_all([project_to_add])
        session.commit()
        session.refresh(project_to_add)
        return ProjectResponse.model_validate(project_to_add)


@router.get("/projects/{id}", tags=["projects"], response_model=ProjectResponse)
def read(id: int):
    with SessionLocal() as session:
        project = session.query(Project).filter(Project.id == id).first()
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")
        return ProjectResponse.model_validate(project)


@router.put("/projects/{id}", tags=["projects"], response_model=ProjectResponse)
def update(id: int, name: str = Form(), description: str = Form(), user_id: int = Form()):
    with SessionLocal() as session:
        project = session.query(Project).filter(Project.id == id).first()
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")
        # ユーザーの存在確認
        user = session.query(User).filter(User.id == user_id).first()
        if not user:
            raise HTTPException(status_code=400, detail=f"User with id {user_id} not found")
        project.name = name
        project.user_id = user_id
        project.updated_at = dt.datetime.now()
        session.commit()
        session.refresh(project)
        return ProjectResponse.model_validate(project)


@router.patch("/projects/{id}", tags=["projects"], response_model=ProjectResponse)
def patch(id: int, attribute: str = Form(), new_value: str = Form()):
    with SessionLocal() as session:
        project = session.query(Project).filter(Project.id == id).first()
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")
        match attribute:
            case "name":
                project.name = new_value
            case "description":
                project.description = new_value
            case "user_id":
                # ユーザーの存在確認
                user = session.query(User).filter(User.id == new_value).first()
                if not user:
                    raise HTTPException(status_code=400, detail=f"User with id {new_value} not found")
                project.user_id = new_value
            case _:
                raise HTTPException(status_code=400, detail="Invalid attribute")
        project.updated_at = dt.datetime.now()
        session.commit()
        session.refresh(project)
        return ProjectResponse.model_validate(project)


@router.delete("/projects/{id}", tags=["projects"])
def delete(id: int):
    with SessionLocal() as session:
        project = session.query(Project).filter(Project.id == id).first()
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")
        session.delete(project)
        session.commit()
        return {"message": "Project deleted successfully"}
