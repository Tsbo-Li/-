import os
import sys
from unittest.mock import MagicMock

from sqlalchemy import text

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

from configs.database_cfg import DatabaseConfig
from database.base_repository import BaseRepository
from database.models import StudentMetric, StudentProfile
from database.student_data_repository import StudentDataRepository
from database.student_profile_repository import StudentProfileRepository


def demo_base_repository(db_uri: str) -> None:
    print("\n=== BaseRepository 演示 ===")
    repo = BaseRepository(db_uri)

    print("[1] run_write 成功路径：")
    mock_session = MagicMock()
    repo.Session = MagicMock(return_value=mock_session)
    result = repo.run_write(lambda session: "ok")
    print(f"    返回值: {result}")
    print(f"    commit调用次数: {mock_session.commit.call_count}")
    print(f"    rollback调用次数: {mock_session.rollback.call_count}")
    print(f"    close调用次数: {mock_session.close.call_count}")

    print("[2] run_write 异常回滚路径：")
    mock_session = MagicMock()
    repo.Session = MagicMock(return_value=mock_session)
    try:
        repo.run_write(lambda session: (_ for _ in ()).throw(RuntimeError("boom")))
    except RuntimeError as exc:
        print(f"    捕获到异常: {exc}")
    print(f"    commit调用次数: {mock_session.commit.call_count}")
    print(f"    rollback调用次数: {mock_session.rollback.call_count}")
    print(f"    close调用次数: {mock_session.close.call_count}")

    print("[3] read_sql_to_df 查询演示（读取真实业务数据，需数据库可连且已导入数据）：")
    real_repo = BaseRepository(db_uri)
    try:
        # 1) 先拿到一个真实存在的 student_id（避免你手写的ID在库里不存在）
        id_df = real_repo.read_sql_to_df(text("SELECT student_id FROM student_metrics LIMIT 1"))
        if id_df.empty:
            print("    当前 student_metrics 表为空：请先运行 scripts/init_db.py 导入仿真数据")
        else:
            student_id = str(id_df.iloc[0]["student_id"])
            print(f"    自动选择 student_id: {student_id}")

            # 2) 查该学生的指标 + 画像（JOIN）
            detail_df = real_repo.read_sql_to_df(
                text(
                    """
                    SELECT
                      m.student_id,
                      m.gpa,
                      m.failed_courses,
                      m.library_visits_per_month,
                      m.late_return_count,
                      m.gaming_traffic_ratio,
                      m.breakfast_frequency,
                      p.basic_tags,
                      p.behavior_tags,
                      p.cognitive_tags,
                      p.radar_scores,
                      p.intervention_action,
                      p.last_computed_at
                    FROM student_metrics m
                    LEFT JOIN student_profiles p ON p.student_id = m.student_id
                    WHERE m.student_id = :sid
                    """
                ),
                params={"sid": student_id},
            )
            print(f"    指标/画像查询结果: {detail_df.to_dict(orient='records')}")

            # 3) 再查几条文本（可选）
            texts_df = real_repo.read_sql_to_df(
                text(
                    """
                    SELECT text_id, content, source_platform, created_at
                    FROM student_texts
                    WHERE student_id = :sid
                    ORDER BY text_id
                    LIMIT 5
                    """
                ),
                params={"sid": student_id},
            )
            print(f"    文本(前5条): {texts_df.to_dict(orient='records')}")
    except Exception as exc:
        print(f"    跳过查询（数据库不可用）: {exc}")
    finally:
        real_repo.close()
        repo.close()


def demo_student_data_repository(db_uri: str) -> None:
    print("\n=== StudentDataRepository 演示 ===")
    repo = StudentDataRepository(db_uri)

    print("[1] load_query 查询演示（需数据库可连）：")
    try:
        df = repo.load_query("SELECT 2 AS value")
        print(f"    查询结果: {df.to_dict(orient='records')}")
    except Exception as exc:
        print(f"    跳过查询（数据库不可用）: {exc}")

    print("[2] insert_metric 使用演示：")
    added_items = []

    def fake_run_write(callback):
        session = MagicMock()
        callback(session)
        added_items.append(session.add.call_args.args[0])

    repo.run_write = fake_run_write
    repo.insert_metric({"student_id": "STU_001", "gpa": 3.5})
    metric_obj = added_items[0]
    print(f"    构造对象类型: {type(metric_obj).__name__}")
    print(f"    student_id: {metric_obj.student_id}, gpa: {metric_obj.gpa}")

    print("[3] insert_text 使用演示：")
    added_items.clear()
    repo.insert_text({"student_id": "STU_001", "content": "hello", "source_platform": "test"})
    text_obj = added_items[0]
    print(f"    构造对象类型: {type(text_obj).__name__}")
    print(f"    student_id: {text_obj.student_id}, content: {text_obj.content}")

    repo.close()


def demo_student_profile_repository(db_uri: str) -> None:
    print("\n=== StudentProfileRepository 演示 ===")
    repo = StudentProfileRepository(db_uri)

    print("[1] _to_data 使用演示：")
    data = {"student_id": "STU_001", "basic_tags": ["理工类"]}
    normalized = repo._to_data(data)
    print(f"    输入: {data}")
    print(f"    输出: {normalized}")

    print("[2] upsert_profile 缺 student_id 异常演示：")
    try:
        repo.upsert_profile({"basic_tags": ["x"]})
    except ValueError as exc:
        print(f"    捕获异常: {exc}")

    print("[3] upsert_profile 插入路径演示：")
    added_items = []

    class FakeInsertSession:
        def get(self, model, student_id):
            return None

        def add(self, obj):
            added_items.append(obj)

    repo.run_write = lambda callback: callback(FakeInsertSession())
    repo.upsert_profile({"student_id": "STU_001", "basic_tags": ["理工类"]})
    print(f"    本次add对象: {[type(x).__name__ for x in added_items]}")

    print("[4] upsert_profile 更新路径演示：")
    existing_profile = StudentProfile(student_id="STU_001", basic_tags=["旧标签"])

    class FakeUpdateSession:
        def get(self, model, student_id):
            if model is StudentMetric:
                return StudentMetric(student_id=student_id)
            if model is StudentProfile:
                return existing_profile
            return None

        def add(self, obj):
            raise RuntimeError("更新路径不应新增画像对象")

    repo.run_write = lambda callback: callback(FakeUpdateSession())
    repo.upsert_profile(
        {
            "student_id": "STU_001",
            "basic_tags": ["新标签"],
            "behavior_tags": ["高频活跃"],
            "cognitive_tags": ["模拟_考研压力"],
        }
    )
    print(f"    更新后 basic_tags: {existing_profile.basic_tags}")
    print(f"    更新后 behavior_tags: {existing_profile.behavior_tags}")
    print(f"    更新后 cognitive_tags: {existing_profile.cognitive_tags}")

    repo.close()


def main() -> None:
    db_uri = DatabaseConfig.get_pg_uri()
    print(f"[配置] 当前数据库URL: {db_uri}")
    print("测试开始。")

    demo_base_repository(db_uri)
    demo_student_data_repository(db_uri)
    demo_student_profile_repository(db_uri)

    print("\n=== 测试结束 ===")


if __name__ == "__main__":
    main()
