"""HALデータモデル定義

Hybrid Access Layerで使用するEnum、データクラスを定義。
"""

from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from datetime import datetime


class StorageMode(Enum):
    """ストレージモード"""
    S3 = "s3"
    LOCAL = "local"
    HYBRID = "hybrid"    # S3とDBの両方にデータがある場合
    UNKNOWN = "unknown"  # storage_mode=nullの場合

    @classmethod
    def from_string(cls, value: Optional[str]) -> 'StorageMode':
        """文字列からStorageModeを取得（nullはUNKNOWN）"""
        if value is None:
            return cls.UNKNOWN
        try:
            return cls(value.lower())
        except ValueError:
            return cls.UNKNOWN


class ContentType(Enum):
    """コンテンツ種別"""
    OPERATION_LOG = "operation_log"      # オペレーションログ
    PROTOCOL_YAML = "protocol_yaml"      # プロトコルYAML
    MANIPULATE_YAML = "manipulate_yaml"  # 操作定義YAML
    PROCESS_DATA = "process_data"        # プロセスデータ
    MEASUREMENT = "measurement"          # 測定結果
    OTHER = "other"                       # その他


class DataSource(Enum):
    """データソース種別"""
    FILE = "file"       # ファイルシステム (S3 or LocalFS)
    DATABASE = "db"     # データベース (SQLite)
    VIRTUAL = "virtual" # 仮想ディレクトリ


@dataclass
class ContentItem:
    """仮想ファイルシステムアイテム"""
    name: str                    # ファイル/ディレクトリ名
    path: str                    # 仮想パス
    type: str                    # "file" or "directory"
    size: int                    # バイトサイズ
    last_modified: Optional[str] # 最終更新日時 (ISO 8601形式)
    content_type: ContentType    # コンテンツ種別
    source: DataSource           # データソース
    backend: Optional[str] = None  # バックエンド種別 ("s3", "local", None)

    def to_dict(self) -> Dict[str, Any]:
        """辞書形式に変換（APIレスポンス用）"""
        result = {
            "name": self.name,
            "path": self.path,
            "type": self.type,
            "size": self.size,
            "lastModified": self.last_modified,
            "contentType": self.content_type.value,
            "source": self.source.value
        }
        if self.backend:
            result["backend"] = self.backend
        return result


@dataclass
class StorageInfo:
    """ストレージ情報"""
    mode: StorageMode           # ストレージモード
    storage_address: str        # 相対パス (runs/XX/)
    full_path: str              # フルパス (s3://... or db://...)
    data_sources: Dict[str, str] = field(default_factory=dict)  # 各データ種別のソース
    warning: Optional[str] = None  # 警告メッセージ（UNKNOWNモード時など）
    inferred: bool = False      # モードが推論されたかどうか
    is_hybrid: bool = False     # ハイブリッドモードかどうか（S3+DB両方にデータあり）
    is_accessible: bool = True  # ストレージにアクセス可能かどうか
    s3_path: Optional[str] = None    # S3パス（ハイブリッド時）
    local_path: Optional[str] = None  # ローカルパス（ハイブリッド時）

    def to_dict(self) -> Dict[str, Any]:
        """辞書形式に変換（APIレスポンス用）"""
        result = {
            "mode": self.mode.value,
            "storage_address": self.storage_address,
            "full_path": self.full_path,
            "data_sources": self.data_sources,
            "isAccessible": self.is_accessible
        }
        if self.warning:
            result["warning"] = self.warning
        if self.inferred:
            result["inferred"] = True
        if self.is_hybrid:
            result["isHybrid"] = True
            if self.s3_path:
                result["s3Path"] = self.s3_path
            if self.local_path:
                result["localPath"] = self.local_path
        return result
