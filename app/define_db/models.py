from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.types import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy.types import String
from sqlalchemy.types import Text
from sqlalchemy import CheckConstraint, UniqueConstraint, Index
from typing import List, Optional
from define_db.database import Base, engine
from datetime import datetime


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(
        primary_key=True,
        autoincrement=True
        # index=True
    )
    email: Mapped[str] = mapped_column(
        # Gmailアドレスは最大40文字（ユーザー名30文字 + "@gmail.com"10文字）
        # 参考：https://support.google.com/mail/answer/9211434?hl=ja
        # 10文字のバッファをもたせる
        String(50),
        # index=True
    )


class Project(Base):
    __tablename__ = "projects"

    id: Mapped[int] = mapped_column(
        primary_key=True,
        autoincrement=True
        # index=True
    )
    name: Mapped[str] = mapped_column(
        String(256),
    )
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id")
    )
    user: Mapped["User"] = relationship(
        foreign_keys=[user_id]
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(),
    )


class Run(Base):
    __tablename__ = "runs"
    id: Mapped[int] = mapped_column(primary_key=True)
    project_id: Mapped[int] = mapped_column(ForeignKey("projects.id"))
    project: Mapped["Project"] = relationship(
        foreign_keys=[project_id]
    )
    file_name: Mapped[str] = mapped_column(String(256))
    checksum: Mapped[str] = mapped_column(String(256))
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id")
    )
    user: Mapped["User"] = relationship(
        foreign_keys=[user_id]
    )
    added_at: Mapped[datetime] = mapped_column(
        DateTime(),
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(),
        nullable=True,
    )
    finished_at: Mapped[datetime] = mapped_column(
        DateTime(),
        nullable=True,
    )
    status: Mapped[str] = mapped_column(String(10))
    storage_address: Mapped[str] = mapped_column(String(256))
    # ★追加: ストレージモード
    storage_mode: Mapped[str] = mapped_column(
        String(10),
        nullable=True,
        default=None,
        comment="ストレージモード: 's3' または 'local'"
    )
    deleted_at: Mapped[datetime] = mapped_column(
        DateTime(),
        nullable=True,
    )
    display_visible: Mapped[bool] = mapped_column(
        default=True,
        nullable=False,
    )


class Process(Base):
    __tablename__ = "processes"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(256))
    run_id: Mapped[int] = mapped_column(ForeignKey("runs.id"))
    run: Mapped["Run"] = relationship(
        foreign_keys=[run_id]
    )
    storage_address: Mapped[str] = mapped_column(String(256))

    # ★追加: process_typeカラム
    process_type: Mapped[str] = mapped_column(
        String(256),
        nullable=True,
        comment="プロセスタイプ (例: ServePlate96, DispenseLiquid96Wells)"
    )

    # ★追加: Portへの逆参照
    ports: Mapped[List["Port"]] = relationship(
        "Port",
        back_populates="process",
        cascade="all, delete-orphan"
    )

    # ★追加: ProcessOperationへの逆参照（多対多リレーション用）
    process_operations: Mapped[List["ProcessOperation"]] = relationship(
        "ProcessOperation",
        back_populates="process",
        cascade="all, delete-orphan"
    )


class Operation(Base):
    __tablename__ = "operations"
    id: Mapped[int] = mapped_column(primary_key=True)
    process_id: Mapped[int] = mapped_column(ForeignKey("processes.id"))
    process: Mapped["Process"] = relationship(
        foreign_keys=[process_id]
    )
    name: Mapped[str] = mapped_column(String(256))
    parent_id: Mapped[int] = mapped_column(
        ForeignKey("operations.id"),
        nullable=True
    )
    parent: Mapped["Operation"] = relationship(
        foreign_keys=[parent_id]
    )
    started_at: Mapped[datetime] = mapped_column(
        # Timestamp(),
        DateTime(),
        nullable=True
    )
    finished_at: Mapped[datetime] = mapped_column(
        # Timestamp(),
        DateTime(),
        nullable=True
    )
    status: Mapped[str] = mapped_column(String(10))
    storage_address: Mapped[str] = mapped_column(String(256))
    is_transport: Mapped[bool] = mapped_column(
        nullable=False
    )
    is_data: Mapped[bool] = mapped_column(
        nullable=False
    )
    log: Mapped[str] = mapped_column(Text, nullable=True)

    # ★追加: ProcessOperationへの逆参照（多対多リレーション用）
    process_operations: Mapped[List["ProcessOperation"]] = relationship(
        "ProcessOperation",
        back_populates="operation",
        cascade="all, delete-orphan"
    )


class Edge(Base):
    __tablename__ = "edges"
    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("runs.id"))
    run: Mapped["Run"] = relationship(
        foreign_keys=[run_id]
    )
    from_id: Mapped[int] = mapped_column(ForeignKey("operations.id"))
    from_: Mapped["Operation"] = relationship(
        foreign_keys=[from_id]
    )
    to_id: Mapped[int] = mapped_column(ForeignKey("operations.id"))
    to: Mapped["Operation"] = relationship(
        foreign_keys=[to_id]
    )


class Port(Base):
    """ポート情報テーブル

    プロセスの入出力ポート情報を管理する。
    YAMLファイル(manipulate.yaml, protocol.yaml)から抽出された
    ポート定義をデータベースに保存する。
    """
    __tablename__ = "ports"

    # 主キー
    id: Mapped[int] = mapped_column(
        primary_key=True,
        autoincrement=True
    )

    # 外部キー: 所属プロセス
    process_id: Mapped[int] = mapped_column(
        ForeignKey("processes.id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )
    process: Mapped["Process"] = relationship(
        "Process",
        foreign_keys=[process_id],
        back_populates="ports"
    )

    # ポート基本情報
    port_name: Mapped[str] = mapped_column(
        String(256),
        nullable=False,
        comment="ポート名(例: value, in1, config)"
    )

    port_type: Mapped[str] = mapped_column(
        String(10),
        nullable=False,
        comment="方向: input または output"
    )

    data_type: Mapped[str] = mapped_column(
        String(256),
        nullable=False,
        comment="データ型(例: Plate96, PlateConfig)"
    )

    # ポート順序
    position: Mapped[int] = mapped_column(
        nullable=False,
        default=0,
        comment="ポート順序(同一プロセス内での並び順)"
    )

    # ポート属性(拡張カラム)
    is_required: Mapped[bool] = mapped_column(
        nullable=False,
        default=True,
        comment="必須フラグ"
    )

    default_value: Mapped[str] = mapped_column(
        Text,
        nullable=True,
        comment="デフォルト値(JSON形式)"
    )

    description: Mapped[str] = mapped_column(
        Text,
        nullable=True,
        comment="ポートの説明"
    )

    # 制約
    __table_args__ = (
        CheckConstraint("port_type IN ('input', 'output')", name="check_port_type"),
        UniqueConstraint("process_id", "port_type", "port_name", name="unique_process_port"),
    )


class ProcessOperation(Base):
    """プロセス-オペレーション間リレーションテーブル

    プロセスとオペレーションの多対多の関係を管理する。
    2025年11月6日MTG決定事項に基づき作成。

    目的:
    - プロセスに紐づくオペレーションを効率的にフィルタリング
    - ランでフィルタリング → プロセスでフィルタリングの2段階検索を実現
    """
    __tablename__ = "process_operations"

    # 主キー
    id: Mapped[int] = mapped_column(
        primary_key=True,
        autoincrement=True
    )

    # 外部キー: Process
    process_id: Mapped[int] = mapped_column(
        ForeignKey("processes.id", ondelete="CASCADE"),
        nullable=False
    )
    process: Mapped["Process"] = relationship(
        "Process",
        foreign_keys=[process_id],
        back_populates="process_operations"
    )

    # 外部キー: Operation
    operation_id: Mapped[int] = mapped_column(
        ForeignKey("operations.id", ondelete="CASCADE"),
        nullable=False
    )
    operation: Mapped["Operation"] = relationship(
        "Operation",
        foreign_keys=[operation_id],
        back_populates="process_operations"
    )

    # 作成日時
    created_at: Mapped[datetime] = mapped_column(
        DateTime(),
        default=datetime.utcnow
    )

    # インデックスとユニーク制約
    __table_args__ = (
        Index('ix_process_operations_process_id', 'process_id'),
        Index('ix_process_operations_operation_id', 'operation_id'),
        UniqueConstraint('process_id', 'operation_id', name='uq_process_operation'),
    )


class PortConnection(Base):
    """ポート接続情報テーブル

    ポート間の接続関係を管理する。
    1つのRunにおける、あるプロセスのoutputポートから
    別のプロセスのinputポートへの接続を表現する。
    """
    __tablename__ = "port_connections"

    # 主キー
    id: Mapped[int] = mapped_column(
        primary_key=True,
        autoincrement=True
    )

    # 外部キー: Run
    run_id: Mapped[int] = mapped_column(
        ForeignKey("runs.id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )
    run: Mapped["Run"] = relationship(
        "Run",
        foreign_keys=[run_id]
    )

    # 外部キー: 出力元ポート
    source_port_id: Mapped[int] = mapped_column(
        ForeignKey("ports.id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )
    source_port: Mapped["Port"] = relationship(
        "Port",
        foreign_keys=[source_port_id]
    )

    # 外部キー: 入力先ポート
    target_port_id: Mapped[int] = mapped_column(
        ForeignKey("ports.id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )
    target_port: Mapped["Port"] = relationship(
        "Port",
        foreign_keys=[target_port_id]
    )

    # 制約
    __table_args__ = (
        UniqueConstraint("run_id", "source_port_id", "target_port_id", name="unique_connection"),
    )


if __name__ == "__main__":
    Base.metadata.create_all(engine)
    print("Table created.")
