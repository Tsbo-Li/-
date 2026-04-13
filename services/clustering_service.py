from typing import Dict, List


class ClusteringService:
    """
    预留 DBSCAN 等聚类能力，当前先保留规则引擎版标签产出。
    """

    def infer_behavior_tags(self, features: Dict[str, float]) -> List[str]:
        tags: List[str] = []
        login_count = features.get("login_count", 0)

        if login_count > 20:
            tags.append("高频活跃")
        elif login_count < 5:
            tags.append("平台潜水")
        return tags
