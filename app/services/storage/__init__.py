"""Storage Module - 統合ストレージサービス

S3とローカルファイルシステムを統一的に扱うためのストレージ抽象化レイヤー。
責任分離型設計: Read + 管理機能を提供（Write操作はlabcode-simが担当）
"""

from .config import StorageConfig, S3Config, LocalConfig
from .registry import BackendRegistry
from .service import StorageService, get_storage
from .backends.base import StorageBackend

__all__ = [
    'StorageConfig',
    'S3Config',
    'LocalConfig',
    'BackendRegistry',
    'StorageService',
    'get_storage',
    'StorageBackend'
]

__version__ = '1.0.0'
