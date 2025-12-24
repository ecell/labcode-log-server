"""DBデータバックエンド

SQLiteデータベースからデータを取得するバックエンド。
ローカルモードでは、オペレーションログ等がDBに保存されている。
"""

import re
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session

from .models import ContentItem, ContentType, DataSource

logger = logging.getLogger(__name__)


class DBDataBackend:
    """データベースからデータを取得するバックエンド"""

    def __init__(self, db_session: Session):
        self._db = db_session

    def list_operation_logs(self, run_id: int, prefix: str = "") -> List[ContentItem]:
        """
        Run内のオペレーションログを仮想ファイルとして一覧取得

        階層構造を尊重し、現在の階層に属するファイルのみを返す:
        - prefix="": 何も返さない（ディレクトリのみ）
        - prefix="operations/": 何も返さない（サブディレクトリのみ）
        - prefix="operations/172/": log.txtを返す

        Args:
            run_id: Run ID
            prefix: フィルタリング用プレフィックス

        Returns:
            ContentItemのリスト
        """
        from define_db.models import Operation, Process

        items = []

        # ルート階層または operations/ 階層の場合は、ファイルは返さない
        # （ディレクトリは list_virtual_directories で返す）
        if prefix == "" or prefix == "operations/" or prefix == "operations":
            return items

        # operations/{op_id}/ 階層の場合のみ、log.txt を返す
        op_id_match = re.match(r'^operations/(\d+)/?$', prefix)
        if not op_id_match:
            return items

        target_op_id = int(op_id_match.group(1))

        # 該当オペレーションを取得
        operation = self._db.query(Operation).filter(
            Operation.id == target_op_id
        ).first()

        if operation and operation.log:
            # プロセスがこのRunに属しているか確認
            process = self._db.query(Process).filter(
                Process.id == operation.process_id,
                Process.run_id == run_id
            ).first()

            if process:
                log_path = f"operations/{operation.id}/log.txt"
                items.append(ContentItem(
                    name="log.txt",
                    path=log_path,
                    type="file",
                    size=len(operation.log.encode('utf-8')),
                    last_modified=operation.finished_at.isoformat() if operation.finished_at else None,
                    content_type=ContentType.OPERATION_LOG,
                    source=DataSource.DATABASE
                ))

        return items

    def list_virtual_directories(self, run_id: int, prefix: str = "") -> List[ContentItem]:
        """
        仮想ディレクトリ一覧を生成

        Args:
            run_id: Run ID
            prefix: フィルタリング用プレフィックス

        Returns:
            仮想ディレクトリのContentItemリスト
        """
        from define_db.models import Operation, Process

        items = []

        # 該当Runのオペレーションを取得
        operations = self._db.query(Operation).join(Process).filter(
            Process.run_id == run_id
        ).all()

        op_ids_with_log = [op.id for op in operations if op.log]

        # ルートレベルの場合: operations/ディレクトリを追加
        if prefix == "" and op_ids_with_log:
            items.append(ContentItem(
                name="operations",
                path="operations/",
                type="directory",
                size=0,
                last_modified=None,
                content_type=ContentType.OTHER,
                source=DataSource.VIRTUAL
            ))

        # operations/レベルの場合: 各オペレーションのサブディレクトリを追加
        if prefix == "operations/" or prefix == "operations":
            for op_id in op_ids_with_log:
                items.append(ContentItem(
                    name=str(op_id),
                    path=f"operations/{op_id}/",
                    type="directory",
                    size=0,
                    last_modified=None,
                    content_type=ContentType.OTHER,
                    source=DataSource.VIRTUAL
                ))

        return items

    def load_operation_log(self, operation_id: int) -> Optional[bytes]:
        """
        オペレーションログを取得

        Args:
            operation_id: Operation ID

        Returns:
            ログ内容のバイト列（なければNone）
        """
        from define_db.models import Operation

        operation = self._db.query(Operation).filter(
            Operation.id == operation_id
        ).first()

        if operation and operation.log:
            return operation.log.encode('utf-8')
        return None

    def get_operation_log_info(self, operation_id: int) -> Optional[Dict[str, Any]]:
        """
        オペレーションログの情報を取得

        Args:
            operation_id: Operation ID

        Returns:
            ログ情報の辞書
        """
        from define_db.models import Operation

        operation = self._db.query(Operation).filter(
            Operation.id == operation_id
        ).first()

        if not operation or not operation.log:
            return None

        return {
            "size": len(operation.log.encode('utf-8')),
            "last_modified": operation.finished_at.isoformat() if operation.finished_at else None,
            "operation_name": operation.name
        }

    @staticmethod
    def extract_operation_id(path: str) -> Optional[int]:
        """
        パスからオペレーションIDを抽出

        Args:
            path: 仮想パス (例: "operations/172/log.txt")

        Returns:
            Operation ID（抽出できなければNone）
        """
        match = re.search(r'operations/(\d+)/', path)
        if match:
            return int(match.group(1))
        return None

    @staticmethod
    def is_operation_log_path(path: str) -> bool:
        """
        パスがオペレーションログを指しているか判定

        Args:
            path: 仮想パス

        Returns:
            オペレーションログの場合True
        """
        return "operations/" in path and path.endswith("log.txt")
