"""ストレージ設定クラス

環境変数からの設定読み込みを一元管理。
"""

from dataclasses import dataclass, field
from typing import Optional
import os


@dataclass
class S3Config:
    """S3固有設定"""
    bucket_name: str = "labcode-dev-artifacts"
    endpoint_url: Optional[str] = None
    region: str = "ap-northeast-1"
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None

    @classmethod
    def from_env(cls) -> 'S3Config':
        """環境変数から設定を読み込み"""
        return cls(
            bucket_name=os.getenv('S3_BUCKET_NAME', 'labcode-dev-artifacts'),
            endpoint_url=os.getenv('S3_ENDPOINT_URL'),
            region=os.getenv('AWS_DEFAULT_REGION', 'ap-northeast-1'),
            access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )


@dataclass
class LocalConfig:
    """ローカルストレージ固有設定"""
    base_path: str = "/data/storage"

    @classmethod
    def from_env(cls) -> 'LocalConfig':
        """環境変数から設定を読み込み"""
        return cls(
            base_path=os.getenv('LOCAL_STORAGE_PATH', '/data/storage')
        )


@dataclass
class StorageConfig:
    """統合ストレージ設定"""
    mode: str = "s3"
    s3: S3Config = field(default_factory=S3Config)
    local: LocalConfig = field(default_factory=LocalConfig)

    @classmethod
    def from_env(cls) -> 'StorageConfig':
        """環境変数から設定を読み込み"""
        return cls(
            mode=os.getenv('STORAGE_MODE', 's3').lower(),
            s3=S3Config.from_env(),
            local=LocalConfig.from_env()
        )

    def get_backend_config(self):
        """現在のモードに対応するバックエンド設定を取得"""
        if self.mode == 's3':
            return self.s3
        elif self.mode == 'local':
            return self.local
        return None
