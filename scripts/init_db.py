import json
import os
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

from configs.database_cfg import DatabaseConfig
from database.models import Base, StudentMetric, StudentProfile, StudentText


def init_database_and_seed_data() -> None:
    print("⏳ 正在连接 PostgreSQL 数据库...")
    engine = create_engine(DatabaseConfig.get_pg_uri(), echo=False)

    print("🛠️ 正在同步表结构...")
    Base.metadata.create_all(engine)
    print("✅ 表结构同步完成")

    session = sessionmaker(bind=engine)()
    try:
        if session.query(StudentMetric).first():
            print("⚠️ 数据库中已存在数据，跳过填充")
            return

        mock_data_path = os.path.join(project_root, "data", "mock_students.json")
        if not os.path.exists(mock_data_path):
            print(f"⚠️ 未找到仿真数据文件: {mock_data_path}")
            return

        with open(mock_data_path, "r", encoding="utf-8") as f:
            students_data = json.load(f)

        for data in students_data:
            metrics = data["structured_data"]
            session.add(
                StudentMetric(
                    student_id=data["student_id"],
                    gpa=metrics["gpa"],
                    failed_courses=metrics["failed_courses"],
                    library_visits_per_month=metrics["library_visits_per_month"],
                    late_return_count=metrics["late_return_count"],
                    gaming_traffic_ratio=metrics["gaming_traffic_ratio"],
                    breakfast_frequency=metrics["breakfast_frequency"],
                )
            )

            for text_content in data["unstructured_texts"]:
                session.add(
                    StudentText(
                        student_id=data["student_id"],
                        content=text_content,
                        source_platform="仿真爬虫抓取",
                    )
                )

            session.add(StudentProfile(student_id=data["student_id"]))

        session.commit()
        print(f"🎉 成功写入 {len(students_data)} 条仿真学生数据")
    except Exception as exc:
        session.rollback()
        print(f"❌ 写入失败并已回滚: {exc}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    init_database_and_seed_data()
