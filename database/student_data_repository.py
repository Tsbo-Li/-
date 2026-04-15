from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import pandas as pd
from sqlalchemy import text

from .base_repository import BaseRepository
from .models import StudentMetric, StudentText


class StudentDataRepository(BaseRepository):
    def load_table(
        self,
        table_name: str,
        columns: Optional[list[str]] = None,
        where_clause: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        selected_cols = ", ".join(columns) if columns else "*"
        sql = f"SELECT {selected_cols} FROM {table_name}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        if limit is not None:
            sql += " LIMIT :_limit"
            params = params or {}
            params["_limit"] = int(limit)
        return self.read_sql_to_df(text(sql), params=params)

    def load_query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        return self.read_sql_to_df(text(sql), params=params)

    def load_student_texts(
        self,
        student_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        sql = """
        SELECT text_id, student_id, content, source_platform, created_at
        FROM student_texts
        WHERE student_id = :student_id
        """
        params: Dict[str, Any] = {"student_id": student_id}

        if start_time is not None:
            sql += " AND created_at >= :start_time"
            params["start_time"] = start_time
        if end_time is not None:
            sql += " AND created_at < :end_time"
            params["end_time"] = end_time

        sql += " ORDER BY created_at DESC"
        if limit is not None:
            sql += " LIMIT :_limit"
            params["_limit"] = int(limit)

        return self.read_sql_to_df(text(sql), params=params)

    def build_text_payload(self, text_df: pd.DataFrame, student_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Convert text query DataFrame to preprocessor-ready payload:
        {
            "student_id": "...",
            "unstructured_texts": ["...", "..."]
        }
        """
        if text_df.empty:
            return {"student_id": student_id, "unstructured_texts": []}

        if "content" not in text_df.columns:
            raise ValueError("text_df 缺少 content 列，无法组装 unstructured_texts")

        sid = student_id
        if sid is None and "student_id" in text_df.columns:
            sid = str(text_df.iloc[0]["student_id"])

        texts = [str(content) for content in text_df["content"].tolist() if content is not None]
        return {"student_id": sid, "unstructured_texts": texts}

    def load_student_text_payload(
        self,
        student_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        One-stop helper:
        query student_texts -> return {"student_id", "unstructured_texts"} payload.
        """
        text_df = self.load_student_texts(
            student_id=student_id,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )
        return self.build_text_payload(text_df, student_id=student_id)

    # data是要插入的数据，为dict类型或者pydantic对象
    def insert_metric(self, data: Dict[str, Any]) -> None:
        def _write(session) -> None:
            session.add(StudentMetric(**data))
        self.run_write(_write)

    def insert_text(self, data: Dict[str, Any]) -> None:
        def _write(session) -> None:
            session.add(StudentText(**data))
        self.run_write(_write)


# class DataLoader(StudentDataRepository):
#     # Backward-compatible alias.
#     pass


def test_student_data_repository_pipeline() -> None:
    print("=== StudentDataRepository 自检 ===")
    repo = StudentDataRepository()

    print("[1] load_query 演示（依赖数据库可连）")
    try:
        df = repo.load_query("SELECT student_id, gpa FROM student_metrics LIMIT 3")
        print(f"    行数: {len(df)}")
        print(f"    输出: {df.to_dict(orient='records')}")
    except Exception as exc:
        print(f"    跳过查询（数据库不可用）: {exc}")

    print("[2] insert_metric 演示（构造数据示例）")
    metric_data = {
        "student_id": "STU_DEMO_DATA_001",
        "gpa": 3.2,
        "failed_courses": 0,
        "library_visits_per_month": 12,
        "late_return_count": 1,
        "gaming_traffic_ratio": 0.15,
        "breakfast_frequency": 0.8,
    }
    print(f"    metric_data: {metric_data}")
    try:
        repo.insert_metric(metric_data)
        print("    insert_metric 执行成功")
    except Exception as exc:
        print(f"    insert_metric 跳过或失败: {exc}")

    print("[3] insert_text 演示（构造数据示例）")
    text_data = {
        "student_id": "STU_DEMO_DATA_001",
        "content": "这是一条用于仓储方法演示的文本",
        "source_platform": "demo",
    }
    print(f"    text_data: {text_data}")
    try:
        repo.insert_text(text_data)
        print("    insert_text 执行成功")
    except Exception as exc:
        print(f"    insert_text 跳过或失败: {exc}")

    print("[4] 插入后查询验证（依赖数据库可连）")
    try:
        verify_df = repo.load_query(
            "SELECT student_id, gpa FROM student_metrics WHERE student_id = :sid",
            params={"sid": "STU_DEMO_DATA_001"},
        )
        print(f"    metric验证: {verify_df.to_dict(orient='records')}")
        verify_text_df = repo.load_query(
            "SELECT student_id, content, source_platform FROM student_texts WHERE student_id = :sid LIMIT 3",
            params={"sid": "STU_DEMO_DATA_001"},
        )
        print(f"    text验证: {verify_text_df.to_dict(orient='records')}")
    except Exception as exc:
        print(f"    验证查询跳过或失败: {exc}")

    print("[5] load_student_texts 时间条件演示（依赖数据库可连）")
    try:
        now = datetime.now()
        one_day_ago = now - timedelta(days=1)
        recent_texts_df = repo.load_student_texts(
            student_id="STU_DEMO_DATA_001",
            start_time=one_day_ago,
            end_time=now,
            limit=5,
        )
        print(f"    时间窗口: [{one_day_ago.isoformat()}, {now.isoformat()})")
        print(f"    recent_texts输出: {recent_texts_df.to_dict(orient='records')}")
        payload = repo.build_text_payload(recent_texts_df, student_id="STU_DEMO_DATA_001")
        print(f"    build_text_payload输出: {payload}")
        direct_payload = repo.load_student_text_payload(
            student_id="STU_DEMO_DATA_001",
            start_time=one_day_ago,
            end_time=now,
            limit=5,
        )
        print(f"    load_student_text_payload输出: {direct_payload}")
    except Exception as exc:
        print(f"    load_student_texts 跳过或失败: {exc}")
    print("=== StudentDataRepository 自检结束 ===")
    repo.close()


if __name__ == "__main__":
    test_student_data_repository_pipeline()
