"""ストレージバックエンド抽象基底クラス

すべてのストレージバックエンドが実装すべきインターフェースを定義。
責任分離型設計: Read操作を中心とし、Write操作はオプショナル。
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Generator, Dict, Any


class StorageBackend(ABC):
    """ストレージバックエンドの抽象基底クラス（Read専用重視）"""

    # --- 読み取り系メソッド（Read Operations） ---

    @abstractmethod
    def load(self, path: str) -> Optional[bytes]:
        """
        ファイルを読み込む

        Args:
            path: ファイルパス（相対パス形式）

        Returns:
            Optional[bytes]: ファイル内容、存在しない場合はNone
        """
        pass

    @abstractmethod
    def load_stream(self, path: str, chunk_size: int = 65536) -> Generator[bytes, None, None]:
        """
        ファイルをストリーミング読み込みする

        Args:
            path: ファイルパス
            chunk_size: チャンクサイズ（デフォルト64KB）

        Yields:
            bytes: ファイルチャンク
        """
        pass

    @abstractmethod
    def list_objects(self, prefix: str) -> List[Dict[str, Any]]:
        """
        指定プレフィックス配下のオブジェクト一覧を取得する

        Args:
            prefix: プレフィックス

        Returns:
            List[Dict]: オブジェクト情報リスト
                各要素: {'Key': str, 'Size': int, 'LastModified': datetime}
        """
        pass

    @abstractmethod
    def list_objects_with_dirs(self, prefix: str, delimiter: str = '/') -> Dict[str, Any]:
        """
        ファイルとディレクトリの一覧を取得する

        Args:
            prefix: プレフィックス
            delimiter: パス区切り文字

        Returns:
            Dict: {'contents': [...], 'common_prefixes': [...]}
        """
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        """
        ファイルが存在するか確認する

        Args:
            path: ファイルパス

        Returns:
            bool: 存在する場合True
        """
        pass

    @abstractmethod
    def get_metadata(self, path: str) -> Optional[Dict[str, Any]]:
        """
        ファイルのメタデータを取得する

        Args:
            path: ファイルパス

        Returns:
            Optional[Dict]: メタデータ（content_length, last_modified等）
        """
        pass

    # --- 書き込み系メソッド（Optional - log-serverでは基本使用しない） ---

    def save(self, path: str, content: bytes, content_type: str = 'application/octet-stream') -> bool:
        """
        ファイルを保存する（オプショナル）

        Args:
            path: 保存先パス（相対パス形式）
            content: ファイル内容（バイト列）
            content_type: コンテンツタイプ

        Returns:
            bool: 成功時True
        """
        raise NotImplementedError("Write operations are optional for log-server backends")

    def delete(self, path: str) -> bool:
        """
        ファイルを削除する（オプショナル）

        Args:
            path: ファイルパス

        Returns:
            bool: 成功時True
        """
        raise NotImplementedError("Write operations are optional for log-server backends")

    # --- オプショナルメソッド（Optional Operations） ---

    def generate_presigned_url(self, path: str, expires_in: int = 3600) -> Optional[str]:
        """
        事前署名URLを生成する（S3のみ実装）

        Args:
            path: ファイルパス
            expires_in: 有効期限（秒）

        Returns:
            Optional[str]: 事前署名URL、未対応の場合None
        """
        return None
