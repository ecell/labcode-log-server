from pydantic import BaseModel, ConfigDict
from typing import Optional, List
from datetime import datetime


class UserResponse(BaseModel):
    id: int
    email: str

    class Config:
        from_attributes = True


class ProjectResponse(BaseModel):
    id: int
    name: str
    user_id: int
    created_at: datetime
    updated_at: datetime
    # user: Optional[UserResponse]  # リレーション

    class Config:
        from_attributes = True


class RunResponse(BaseModel):
    id: int
    project_id: int
    file_name: str
    checksum: str
    user_id: int
    added_at: datetime
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    status: str
    storage_address: str
    deleted_at: datetime | None
    # project: Optional[ProjectResponse]  # リレーション
    # user: Optional[UserResponse]  # リレーション

    class Config:
        from_attributes = True


class RunResponseWithProjectName(BaseModel):
    id: int
    project_id: int
    project_name: str
    file_name: str
    checksum: str
    user_id: int
    added_at: datetime
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    status: str
    storage_address: str
    # project: Optional[ProjectResponse]  # リレーション
    # user: Optional[UserResponse]  # リレーション

    class Config:
        from_attributes = True


class ProcessResponse(BaseModel):
    id: int
    name: str
    run_id: int
    storage_address: str

    class Config:
        from_attributes = True


class OperationResponse(BaseModel):
    id: int
    name: str
    parent_id: Optional[int]
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    status: str
    storage_address: str
    is_transport: bool
    is_data: bool
    log: Optional[str]
    # process: Optional["ProcessResponse"]  # リレーション
    # parent: Optional["OperationResponse"]  # 自己リレーション

    class Config:
        from_attributes = True

# # 自己参照モデルのための更新
# OperationResponse.update_forward_refs()


class OperationResponseWithProcessStorageAddress(BaseModel):
    id: int
    name: str
    process_id: int
    process_name: str
    parent_id: Optional[int]
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    status: str
    storage_address: str
    process_storage_address: str
    is_transport: bool
    log: Optional[str]
    # process: Optional["ProcessResponse"]  # リレーション
    # parent: Optional["OperationResponse"]  # 自己リレーション

    class Config:
        from_attributes = True

# # 自己参照モデルのための更新
# OperationResponse.update_forward_refs()


class EdgeResponse(BaseModel):
    id: int
    run_id: int
    from_id: int
    to_id: int
    # from_: Optional[OperationResponse]  # リレーション
    # to: Optional[OperationResponse]  # リレーション

    class Config:
        from_attributes = True


# ============================================================
# Process API用の新規レスポンスモデル (TODO Step 1.1対応)
# ============================================================

class PortResponse(BaseModel):
    """ポート情報のレスポンスモデル"""
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    data_type: str
    connected_to: Optional[str] = None
    connected_from: Optional[str] = None


class PortsResponse(BaseModel):
    """入出力ポート情報のレスポンスモデル"""
    input: Optional[List[PortResponse]] = None
    output: Optional[List[PortResponse]] = None


class PortDetailResponse(BaseModel):
    """ポート詳細情報のレスポンスモデル（新規API用）"""
    model_config = ConfigDict(from_attributes=True)

    id: int
    process_id: int
    port_name: str
    port_type: str  # "input" | "output"
    data_type: str
    position: int
    is_required: bool
    default_value: Optional[str] = None
    description: Optional[str] = None


class PortConnectionResponse(BaseModel):
    """ポート接続情報のレスポンスモデル（新規API用）"""
    connection_id: int
    run_id: int
    source_process_id: int
    source_process_name: str
    source_port_id: int
    source_port_name: str
    target_process_id: int
    target_process_name: str
    target_port_id: int
    target_port_name: str


class ProcessResponseEnhanced(BaseModel):
    """プロセス基本情報のレスポンスモデル（拡張版）

    既存のProcessResponseとの違い:
    - type, status, created_at, updated_atフィールドを追加
    - started_at, finished_atフィールドを追加（Runから取得）
    - ConfigDict(from_attributes=True)を使用（Pydantic v2）

    注意: DBのProcessモデルにはtype/status/created_at/updated_atフィールドが
    存在しないため、APIレイヤーで動的に設定する必要がある
    started_at/finished_atはRunテーブルから取得する
    """
    model_config = ConfigDict(from_attributes=True)

    id: int
    run_id: int
    name: str
    type: str
    status: str
    created_at: datetime
    updated_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None


class ProcessDetailResponse(ProcessResponseEnhanced):
    """プロセス詳細情報のレスポンスモデル（ポート情報含む）

    ProcessResponseEnhancedを継承し、以下を追加:
    - ports: ポート情報（YAML動的読み込み）
    - storage_address: ストレージアドレス
    - started_at: 開始日時
    - finished_at: 終了日時
    """
    ports: Optional[PortsResponse] = None
    storage_address: Optional[str] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None


class ProcessListResponse(BaseModel):
    """プロセス一覧のレスポンスモデル

    ページネーション対応のプロセス一覧レスポンス:
    - total: 総プロセス数
    - items: プロセスリスト（ProcessResponseEnhanced）
    """
    total: int
    items: List[ProcessResponseEnhanced]