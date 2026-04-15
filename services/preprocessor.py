import re
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import jieba
import pandas as pd
from sklearn.preprocessing import StandardScaler

from database.student_data_repository import StudentDataRepository


class Preprocessor:
    def __init__(
        self,
        feature_cols: Optional[list[str]] = None,
        numeric_bounds: Optional[Dict[str, tuple[Optional[float], Optional[float]]]] = None,
    ):
        # 默认特征列：只包含建模使用的数值字段，不包含 student_id 等标识字段。
        self.default_feature_cols = feature_cols or [
            "gpa",
            "failed_courses",
            "library_visits_per_month",
            "late_return_count",
            "gaming_traffic_ratio",
            "breakfast_frequency",
        ]

        default_bounds: Dict[str, tuple[Optional[float], Optional[float]]] = {
            "gpa": (0.0, 4.0),
            "failed_courses": (0.0, 20.0),
            "library_visits_per_month": (0.0, 100.0),
            "late_return_count": (0.0, 100.0),
            "gaming_traffic_ratio": (0.0, 1.0),
            "breakfast_frequency": (0.0, 1.0),
            # 兼容其他潜在结构化字段（如果在 feature_cols 中使用）
            "math_score": (0.0, 100.0),
            "politics_score": (0.0, 100.0),
            "video_completion_rate": (0.0, 1.0),
            "login_count": (0.0, None),
        }
        merged_bounds = {**default_bounds, **(numeric_bounds or {})}
        # 边界配置与特征列对齐：只保留当前特征列对应的边界。
        self.numeric_bounds: Dict[str, tuple[Optional[float], Optional[float]]] = {
            col: merged_bounds[col] for col in self.default_feature_cols if col in merged_bounds
        }
        self.stop_words = {"的", "了", "和", "是", "就", "都", "而", "及", "与"}
        self._text_noise_pattern = re.compile(r"[^\w\u4e00-\u9fff\s]+")
        self._multi_space_pattern = re.compile(r"\s+")

    def _to_float(self, value: Any, default: float = 0.0) -> float:
        if value is None:
            return default
        if isinstance(value, bool):
            return float(int(value))
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return default
            try:
                return float(stripped)
            except ValueError:
                return default
        return default

    def _clip_value(self, key: str, value: float) -> float:
        lower, upper = self.numeric_bounds.get(key, (None, None))
        if lower is not None and value < lower:
            value = lower
        if upper is not None and value > upper:
            value = upper
        return value

    def clean_numeric(self, raw_data: Dict[str, float]) -> Dict[str, float]:
        cleaned: Dict[str, float] = {}
        for key, value in raw_data.items():
            numeric_value = self._to_float(value, default=0.0)
            numeric_value = self._clip_value(key, numeric_value)
            cleaned[key] = numeric_value
        return cleaned

    def clean_text(self, text: Any) -> str:
        """
        Basic text normalization:
        - cast to string
        - lowercase
        - remove punctuation/noise chars
        - normalize spaces
        """
        if text is None:
            return ""
        normalized = str(text).strip().lower()
        normalized = self._text_noise_pattern.sub(" ", normalized)
        normalized = self._multi_space_pattern.sub(" ", normalized).strip()
        return normalized

    def _extract_text_list(self, raw_data: Dict[str, Any], text_key: str = "unstructured_texts") -> list[str]:
        texts = raw_data.get(text_key, [])
        if texts is None:
            return []
        if not isinstance(texts, list):
            raise TypeError(f"{text_key} 必须是 list[str] 类型")
        return ["" if t is None else str(t) for t in texts]

    def clean_texts(
        self,
        raw_data: Dict[str, Any],
        text_key: str = "unstructured_texts",
        student_id_key: str = "student_id",
    ) -> list[Dict[str, Any]]:
        """
        Unified dict-style text cleaning, aligned with clean_numeric(raw_data).
        """
        texts = self._extract_text_list(raw_data, text_key=text_key)
        student_id = raw_data.get(student_id_key)
        return [{"student_id": student_id, "clean_text": self.clean_text(text)} for text in texts]

    def tokenize_texts(
        self,
        raw_data: Dict[str, Any],
        text_key: str = "unstructured_texts",
        student_id_key: str = "student_id",
    ) -> list[Dict[str, Any]]:
        """
        Unified dict-style text tokenization, aligned with clean_numeric(raw_data).
        """
        texts = self._extract_text_list(raw_data, text_key=text_key)
        student_id = raw_data.get(student_id_key)
        result: list[Dict[str, Any]] = []
        for text in texts:
            clean = self.clean_text(text)
            if not clean:
                result.append({"student_id": student_id, "clean_text": "", "tokens": []})
                continue
            tokens = jieba.lcut(clean)
            filtered = [t for t in tokens if t not in self.stop_words and len(t.strip()) > 1]
            result.append({"student_id": student_id, "clean_text": clean, "tokens": filtered})
        return result

    def prepare_docs_for_topic(
        self,
        raw_data: Dict[str, Any],
        text_key: str = "unstructured_texts",
        student_id_key: str = "student_id",
        min_len: int = 2,
    ) -> list[Dict[str, Any]]:
        """
        Unified dict-style topic docs preparation, aligned with clean_numeric(raw_data).
        """
        texts = self._extract_text_list(raw_data, text_key=text_key)
        student_id = raw_data.get(student_id_key)
        docs: list[Dict[str, Any]] = []
        for idx, text in enumerate(texts):
            clean = self.clean_text(text)
            if len(clean) >= min_len:
                docs.append({"student_id": student_id, "text_index": idx, "clean_doc": clean})
        return docs

    def select_feature_columns(self, df: pd.DataFrame, feature_cols: Optional[list[str]] = None) -> list[str]:
        """
        Select numeric feature columns for modeling.
        Excludes ID/time/non-numeric fields by default.
        """
        candidate_cols = feature_cols or self.default_feature_cols
        existing_cols = [col for col in candidate_cols if col in df.columns]
        if not existing_cols:
            raise ValueError("未找到可用的特征列，请检查输入DataFrame字段")
        return existing_cols

    def clean_numeric_df(self, df: pd.DataFrame, feature_cols: Optional[list[str]] = None) -> pd.DataFrame:
        """
        Clean selected numeric columns only.
        """
        cols = self.select_feature_columns(df, feature_cols)
        cleaned_df = df.copy()
        for col in cols:
            cleaned_df[col] = cleaned_df[col].apply(lambda x: self._clip_value(col, self._to_float(x, default=0.0)))
        return cleaned_df

    def standardize_features(
        self, df: pd.DataFrame, feature_cols: Optional[list[str]] = None
    ) -> tuple[pd.DataFrame, StandardScaler]:
        """
        Standardize selected feature columns with StandardScaler.
        Returns (scaled_dataframe, fitted_scaler).
        """
        cols = self.select_feature_columns(df, feature_cols)
        cleaned_df = self.clean_numeric_df(df, cols)
        scaler = StandardScaler()
        cleaned_df[cols] = scaler.fit_transform(cleaned_df[cols])
        return cleaned_df, scaler


def test_preprocessor_numeric_pipeline() -> None:
    print("=== Preprocessor 数值预处理自检 ===")
    raw_df = pd.DataFrame(
        [
            {
                "student_id": "STU_001",
                "gpa": 3.7,
                "failed_courses": 0,
                "library_visits_per_month": 18,
                "late_return_count": 1,
                "gaming_traffic_ratio": 0.08,
                "breakfast_frequency": 0.91,
            },
            {
                "student_id": "STU_002",
                "gpa": "4.2",  # 超边界字符串，预期裁剪到 4.0
                "failed_courses": None,  # 缺失值，预期变为 0.0
                "library_visits_per_month": "15",
                "late_return_count": -3,  # 小于下界，预期裁剪到 0.0
                "gaming_traffic_ratio": "1.5",  # 超界，预期裁剪到 1.0
                "breakfast_frequency": "",
            },
            {
                "student_id": "STU_003",
                "gpa": "abc",  # 非法字符串，预期变为 0.0
                "failed_courses": 2,
                "library_visits_per_month": 300,  # 超界，预期裁剪到 100.0
                "late_return_count": 9,
                "gaming_traffic_ratio": 0.33,
                "breakfast_frequency": 0.4,
            },
        ]
    )
    print("[原始数据]")
    print(raw_df)

    processor = Preprocessor()
    feature_cols = processor.select_feature_columns(raw_df)
    print(f"\n[特征列] {feature_cols}")

    cleaned_df = processor.clean_numeric_df(raw_df, feature_cols)
    print("\n[清洗后数据]")
    print(cleaned_df[["student_id", *feature_cols]])

    scaled_df, scaler = processor.standardize_features(raw_df, feature_cols)
    print("\n[标准化后数据]")
    print(scaled_df[["student_id", *feature_cols]])

    print("\n[StandardScaler 参数]")
    print(f"mean_: {scaler.mean_}")
    print(f"scale_: {scaler.scale_}")

    print("=== 自检结束 ===")


def test_preprocessor_text_pipeline() -> None:
    print("\n=== Preprocessor 文本预处理自检 ===")
    processor = Preprocessor()
    sample_data = {
        "student_id": "STU_TEXT_001",
        "unstructured_texts": [
            "考研数学复习毫无头绪怎么办？？？",
            "中国AI发展太快了，LLM真的很强！",
            "  THIS is a MIXED Text 123 !!! ",
            None,
            "  ",
        ],
    }

    print("[原始文本]")
    print(sample_data["unstructured_texts"])

    cleaned_texts = processor.clean_texts(sample_data)
    print("\n[clean_text 输出]")
    print(cleaned_texts)

    docs = processor.prepare_docs_for_topic(sample_data, min_len=2)
    print("\n[prepare_docs_for_topic 输出]")
    print(docs)

    tokenized = processor.tokenize_texts(sample_data)
    print("\n[tokenize_texts 输出]")
    print(tokenized)
    print("=== 文本自检结束 ===")


def test_preprocessor_with_real_database_data(limit: int = 10) -> None:
    print("\n=== Preprocessor 真实数据库数据自检 ===")
    repo = StudentDataRepository()
    processor = Preprocessor()
    try:
        metric_df = repo.load_query(
            """
            SELECT
              student_id,
              gpa,
              failed_courses,
              library_visits_per_month,
              late_return_count,
              gaming_traffic_ratio,
              breakfast_frequency
            FROM student_metrics
            LIMIT :limit
            """,
            params={"limit": limit},
        )
        if metric_df.empty:
            print("    student_metrics 无数据，跳过数值真实数据测试")
        else:
            print(f"    student_metrics 读取成功，样本数: {len(metric_df)}")
            feature_cols = processor.select_feature_columns(metric_df)
            cleaned_df = processor.clean_numeric_df(metric_df, feature_cols)
            scaled_df, scaler = processor.standardize_features(metric_df, feature_cols)
            print(f"    数值特征列: {feature_cols}")
            print(f"    清洗后前2条: {cleaned_df[['student_id', *feature_cols]].head(2).to_dict(orient='records')}")
            print(f"    标准化后前2条: {scaled_df[['student_id', *feature_cols]].head(2).to_dict(orient='records')}")
            print(f"    scaler.mean_: {scaler.mean_}")
            print(f"    scaler.scale_: {scaler.scale_}")

        text_df = repo.load_query(
            """
            SELECT student_id, content, created_at
            FROM student_texts
            ORDER BY text_id DESC
            LIMIT :limit
            """,
            params={"limit": limit},
        )
        if text_df.empty:
            print("    student_texts 无数据，跳过文本真实数据测试")
        else:
            print(f"    student_texts 读取成功，样本数: {len(text_df)}")
            sample_student_id = str(text_df.iloc[0]["student_id"])
            now = datetime.now()
            one_week_ago = now - timedelta(days=7)
            raw_data = repo.load_student_text_payload(
                student_id=sample_student_id,
                start_time=one_week_ago,
                end_time=now,
                limit=limit,
            )
            docs = processor.prepare_docs_for_topic(raw_data, min_len=2)
            tokenized = processor.tokenize_texts(raw_data)
            print(f"    选取 student_id: {sample_student_id}")
            print(f"    时间窗口: [{one_week_ago.isoformat()}, {now.isoformat()})")
            print(f"    文本条数: {len(raw_data.get('unstructured_texts', []))}")
            print(f"    payload示例: {raw_data}")
            print(f"    docs前5条: {docs[:5]}")
            print(f"    tokens前5条: {tokenized[:5]}")

        print("=== 真实数据库数据自检结束 ===")
    except Exception as exc:
        print(f"    真实数据库数据自检失败（可能是数据库未启动或连接配置不一致）: {exc}")
    finally:
        repo.close()


if __name__ == "__main__":
    test_preprocessor_numeric_pipeline()
    test_preprocessor_text_pipeline()
    test_preprocessor_with_real_database_data(limit=10)
