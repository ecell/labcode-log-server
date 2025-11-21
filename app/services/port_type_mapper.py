"""
ポート型マッピングサービス

Process TypeからPort定義を取得し、正確なデータ型を提供します。
manipulate.yamlファイルから型定義を読み込みます。
"""

from pathlib import Path
import yaml
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class PortTypeMapper:
    """Process TypeからPort型定義へのマッピングを提供"""

    def __init__(self, manipulate_yaml_path: Optional[str] = None):
        """
        Args:
            manipulate_yaml_path: manipulate.yamlファイルのパス
                                 Noneの場合、デフォルトパスを使用
        """
        self.process_port_map: Dict[str, Dict] = {}
        self._load_manipulate_yaml(manipulate_yaml_path)

    def _load_manipulate_yaml(self, yaml_path: Optional[str] = None):
        """manipulate.yamlから型定義を読み込み"""
        # デフォルトパス（プロジェクトルートのmanipulate.yaml）
        if yaml_path is None:
            # Dockerコンテナ内の場合
            default_paths = [
                Path("/app/manipulate.yaml"),  # Dockerコンテナ内 (appディレクトリ直下)
                Path("/app/../manipulate.yaml"),  # Dockerコンテナ内 (親ディレクトリ)
                Path(__file__).parent.parent.parent.parent / "manipulate.yaml",  # 開発環境
            ]

            for path in default_paths:
                if path.exists():
                    yaml_path = str(path)
                    break

        if yaml_path is None or not Path(yaml_path).exists():
            logger.warning(f"manipulate.yaml not found at {yaml_path}. Port type mapping will use fallback.")
            return

        try:
            with open(yaml_path, 'r', encoding='utf-8') as f:
                manipulate_data = yaml.safe_load(f)

            # Process定義を抽出（refが"Operation"または"IOOperation"のもの）
            for process_def in manipulate_data:
                name = process_def.get('name')
                ref = process_def.get('ref')

                if ref in ('Operation', 'IOOperation', 'BuiltinOperation'):
                    self.process_port_map[name] = {
                        'input': process_def.get('input', []),
                        'output': process_def.get('output', [])
                    }

            logger.info(f"Loaded port type definitions for {len(self.process_port_map)} process types from {yaml_path}")

        except Exception as e:
            logger.error(f"Failed to load manipulate.yaml from {yaml_path}: {e}")

    def get_port_type(self, process_type: str, port_name: str, port_direction: str) -> str:
        """
        指定されたProcess Type, Port Name, Port Direction (input/output) からデータ型を取得

        Args:
            process_type: プロセスタイプ (例: "ServePlate96", "DispenseLiquid96Wells")
            port_name: ポート名 (例: "value", "in1", "out1")
            port_direction: ポート方向 ("input" または "output")

        Returns:
            データ型文字列 (例: "Plate96", "Integer", "SpotArray | Plate96")
            見つからない場合は "Unknown"
        """
        if process_type not in self.process_port_map:
            return "Unknown"

        ports = self.process_port_map[process_type].get(port_direction, [])

        for port_def in ports:
            if port_def.get('id') == port_name:
                return port_def.get('type', 'Unknown')

        return "Unknown"

    def get_all_ports_for_process(self, process_type: str) -> Optional[Dict]:
        """
        指定されたProcess Typeのすべてのポート定義を取得

        Args:
            process_type: プロセスタイプ

        Returns:
            {"input": [...], "output": [...]} または None
        """
        return self.process_port_map.get(process_type)

    def infer_port_type_from_connection(
        self,
        source_process_type: str,
        target_process_type: str,
        connection_index: int = 0
    ) -> tuple[str, str]:
        """
        接続情報からポート型を推測

        Args:
            source_process_type: 接続元のプロセスタイプ
            target_process_type: 接続先のプロセスタイプ
            connection_index: 同一プロセス間の複数接続の場合のインデックス

        Returns:
            (source_port_type, target_port_type) のタプル
        """
        source_ports = self.process_port_map.get(source_process_type, {}).get('output', [])
        target_ports = self.process_port_map.get(target_process_type, {}).get('input', [])

        # インデックスが範囲内の場合、該当するポートの型を返す
        source_type = source_ports[connection_index].get('type', 'Unknown') if connection_index < len(source_ports) else 'Unknown'
        target_type = target_ports[connection_index].get('type', 'Unknown') if connection_index < len(target_ports) else 'Unknown'

        return (source_type, target_type)


# シングルトンインスタンス
_port_type_mapper_instance: Optional[PortTypeMapper] = None


def get_port_type_mapper() -> PortTypeMapper:
    """PortTypeMapperのシングルトンインスタンスを取得"""
    global _port_type_mapper_instance

    if _port_type_mapper_instance is None:
        _port_type_mapper_instance = PortTypeMapper()

    return _port_type_mapper_instance
