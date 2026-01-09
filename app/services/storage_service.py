"""ストレージサービス抽象化レイヤー（labcode-log-server用）

services/storageモジュールを使用した統一ストレージアクセス。
責任分離型設計: Read + 管理機能を提供。

使用例:
    from services.storage_service import StorageService, get_storage

    storage = get_storage()
    content = storage.load("runs/1/log.txt")
    objects = storage.list_objects("runs/1/")
"""

# 内部storageモジュールから再エクスポート
from .storage import (
    StorageService,
    StorageBackend,
    StorageConfig,
    S3Config,
    LocalConfig,
    BackendRegistry,
    get_storage
)

# 後方互換性のため、バックエンドクラスも直接エクスポート
from .storage.backends import (
    S3StorageBackend,
    LocalStorageBackend
)

__all__ = [
    'StorageService',
    'StorageBackend',
    'StorageConfig',
    'S3Config',
    'LocalConfig',
    'BackendRegistry',
    'get_storage',
    'S3StorageBackend',
    'LocalStorageBackend'
]
