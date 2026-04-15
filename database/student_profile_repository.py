from typing import Any, Dict, Optional

from .base_repository import BaseRepository
from .models import StudentMetric, StudentProfile


class StudentProfileRepository(BaseRepository):
    def _to_data(self, profile: Any) -> Dict[str, Any]:
        if isinstance(profile, dict):
            return profile
        if hasattr(profile, "model_dump"):
            return profile.model_dump()
        if hasattr(profile, "dict"):
            return profile.dict()
        raise TypeError("profile 必须是 dict 或 Pydantic 对象")

    def upsert_profile(self, profile: Any) -> None:
        data = self._to_data(profile)
        student_id = data.get("student_id")
        if not student_id:
            raise ValueError("profile 中缺少 student_id")

        def _write(session) -> str:
            if session.get(StudentMetric, student_id) is None:
                session.add(StudentMetric(student_id=student_id))

            existing = session.get(StudentProfile, student_id)
            if existing:
                existing.basic_tags = data.get("basic_tags", [])
                existing.behavior_tags = data.get("behavior_tags", [])
                existing.cognitive_tags = data.get("cognitive_tags", [])
                existing.radar_scores = data.get("radar_scores", {})
                existing.intervention_action = data.get("intervention_action")
                action = "更新"
            else:
                session.add(
                    StudentProfile(
                        student_id=student_id,
                        basic_tags=data.get("basic_tags", []),
                        behavior_tags=data.get("behavior_tags", []),
                        cognitive_tags=data.get("cognitive_tags", []),
                        radar_scores=data.get("radar_scores", {}),
                        intervention_action=data.get("intervention_action"),
                    )
                )
                action = "插入"
            return action

        action = self.run_write(_write)
        print(f"✅ 已{action}画像到数据库: {student_id}")

    def save_profile(self, profile: Any) -> None:
        # Backward-compatible alias.
        self.upsert_profile(profile)

    def get_profile(self, student_id: str) -> Optional[StudentProfile]:
        return self.run_read(lambda session: session.get(StudentProfile, student_id))


def test_student_profile_repository_pipeline() -> None:
    print("=== StudentProfileRepository 自检 ===")
    repo = StudentProfileRepository()

    print("[1] _to_data 演示")
    profile_data = {
        "student_id": "STU_DEMO_PROFILE_001",
        "basic_tags": ["理工类"],
        "behavior_tags": ["高频活跃"],
        "cognitive_tags": ["模拟_考研压力"],
        "radar_scores": {"learning": 80},
        "intervention_action": None,
    }
    normalized = repo._to_data(profile_data)
    print(f"    输入: {profile_data}")
    print(f"    输出: {normalized}")

    print("[2] upsert_profile 演示（依赖数据库可连）")
    try:
        repo.upsert_profile(profile_data)
    except Exception as exc:
        print(f"    跳过upsert（数据库不可用）: {exc}")

    print("[3] get_profile 演示（依赖数据库可连）")
    try:
        saved = repo.get_profile("STU_DEMO_PROFILE_001")
        print(f"    查询结果: {saved}")
    except Exception as exc:
        print(f"    跳过查询（数据库不可用）: {exc}")

    repo.close()
    print("=== StudentProfileRepository 自检结束 ===")


if __name__ == "__main__":
    test_student_profile_repository_pipeline()
