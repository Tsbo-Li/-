from __future__ import annotations

from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler


class ClusteringService:
    """
    Numeric behavior clustering service.

    - fit(df): train clustering model from feature dataframe
    - infer_behavior_tags(features): infer tags for one student
    """

    def __init__(self, feature_cols: Optional[List[str]] = None, n_clusters: int = 3, random_state: int = 42):
        self.feature_cols = feature_cols or [
            "gpa",
            "failed_courses",
            "library_visits_per_month",
            "late_return_count",
            "gaming_traffic_ratio",
            "breakfast_frequency",
            "login_count",
        ]
        self.n_clusters = n_clusters
        self.random_state = random_state
        self.scaler = StandardScaler()
        self.model = KMeans(n_clusters=self.n_clusters, n_init=10, random_state=self.random_state)
        self.cluster_tag_map: Dict[int, List[str]] = {}
        self._fitted = False

    def _select_existing_feature_cols(self, df: pd.DataFrame) -> List[str]:
        cols = [col for col in self.feature_cols if col in df.columns]
        if not cols:
            raise ValueError("输入DataFrame中未找到可用聚类特征列")
        return cols

    def fit(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Train clustering model and return df with cluster labels.
        """
        cols = self._select_existing_feature_cols(df)
        train_df = df.copy()
        train_df[cols] = train_df[cols].fillna(0.0)

        x_scaled = self.scaler.fit_transform(train_df[cols])
        cluster_labels = self.model.fit_predict(x_scaled)
        train_df["cluster_label"] = cluster_labels

        self.cluster_tag_map = self._build_cluster_tag_map(train_df, cols)
        self._fitted = True
        return train_df

    def _build_cluster_tag_map(self, labeled_df: pd.DataFrame, cols: List[str]) -> Dict[int, List[str]]:
        """
        Map each cluster to behavior tags using cluster-level averages.
        """
        tag_map: Dict[int, List[str]] = {}
        grouped = labeled_df.groupby("cluster_label")[cols].mean(numeric_only=True)

        for cluster_id, row in grouped.iterrows():
            tags: List[str] = []
            login = float(row.get("login_count", 0.0))
            gaming_ratio = float(row.get("gaming_traffic_ratio", 0.0))
            library_visits = float(row.get("library_visits_per_month", 0.0))
            late_return = float(row.get("late_return_count", 0.0))
            gpa = float(row.get("gpa", 0.0))

            if login >= 20 or library_visits >= 15:
                tags.append("高频活跃")
            if login <= 5 and library_visits <= 3:
                tags.append("平台潜水")
            if gaming_ratio >= 0.5 and late_return >= 6:
                tags.append("娱乐倾向")
            if gpa >= 3.2 and library_visits >= 10:
                tags.append("学业投入")

            if not tags:
                tags.append("一般活跃")

            tag_map[int(cluster_id)] = tags
        return tag_map

    def _features_to_row(self, features: Dict[str, float]) -> pd.DataFrame:
        row = {col: float(features.get(col, 0.0) or 0.0) for col in self.feature_cols}
        return pd.DataFrame([row])

    def infer_behavior_tags(self, features: Dict[str, float]) -> List[str]:
        """
        Infer behavior tags for one student.

        If model has not been fitted, fallback to rule-based inference.
        """
        if not self._fitted:
            return self._fallback_rule_based_tags(features)

        input_df = self._features_to_row(features)
        cols = self._select_existing_feature_cols(input_df)
        x_scaled = self.scaler.transform(input_df[cols])
        cluster = int(self.model.predict(x_scaled)[0])
        return self.cluster_tag_map.get(cluster, ["一般活跃"])

    def _fallback_rule_based_tags(self, features: Dict[str, float]) -> List[str]:
        """
        Keep compatibility with old pipeline before real fit.
        """
        tags: List[str] = []
        login_count = float(features.get("login_count", 0.0) or 0.0)

        if login_count > 20:
            tags.append("高频活跃")
        elif login_count < 5:
            tags.append("平台潜水")
        else:
            tags.append("一般活跃")
        return tags


def test_clustering_service_pipeline() -> None:
    print("=== ClusteringService 数值聚类自检 ===")
    df = pd.DataFrame(
        [
            {
                "student_id": "STU_001",
                "gpa": 3.8,
                "failed_courses": 0,
                "library_visits_per_month": 20,
                "late_return_count": 1,
                "gaming_traffic_ratio": 0.05,
                "breakfast_frequency": 0.9,
                "login_count": 28,
            },
            {
                "student_id": "STU_002",
                "gpa": 2.1,
                "failed_courses": 2,
                "library_visits_per_month": 2,
                "late_return_count": 10,
                "gaming_traffic_ratio": 0.75,
                "breakfast_frequency": 0.2,
                "login_count": 3,
            },
            {
                "student_id": "STU_003",
                "gpa": 3.0,
                "failed_courses": 1,
                "library_visits_per_month": 8,
                "late_return_count": 4,
                "gaming_traffic_ratio": 0.3,
                "breakfast_frequency": 0.5,
                "login_count": 12,
            },
        ]
    )

    service = ClusteringService(n_clusters=3)
    labeled_df = service.fit(df)
    print("[fit后样本]")
    print(labeled_df[["student_id", "cluster_label"]].to_dict(orient="records"))
    print(f"[cluster_tag_map] {service.cluster_tag_map}")

    new_student = {
        "gpa": 3.4,
        "failed_courses": 0,
        "library_visits_per_month": 16,
        "late_return_count": 2,
        "gaming_traffic_ratio": 0.12,
        "breakfast_frequency": 0.85,
        "login_count": 22,
    }
    tags = service.infer_behavior_tags(new_student)
    print(f"[新样本标签] {tags}")
    print("=== ClusteringService 自检结束 ===")


if __name__ == "__main__":
    test_clustering_service_pipeline()
