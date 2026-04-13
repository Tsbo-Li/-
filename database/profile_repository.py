from typing import Any, Dict

from .base_repository import BaseRepository
from .models import StudentMetric, StudentProfile


class ProfileRepository(BaseRepository):
    def _to_payload(self, profile: Any) -> Dict[str, Any]:
        if isinstance(profile, dict):
            return profile
        if hasattr(profile, "model_dump"):
            return profile.model_dump()
        if hasattr(profile, "dict"):
            return profile.dict()
        raise TypeError("profile 必须是 dict 或 Pydantic 对象")

    def save_profile(self, profile: Any) -> None:
        payload = self._to_payload(profile)
        student_id = payload.get("student_id")
        if not student_id:
            raise ValueError("profile 中缺少 student_id")

        session = self.Session()
        try:
            if session.get(StudentMetric, student_id) is None:
                session.add(StudentMetric(student_id=student_id))

            existing = session.get(StudentProfile, student_id)
            if existing:
                existing.basic_tags = payload.get("basic_tags", [])
                existing.behavior_tags = payload.get("behavior_tags", [])
                existing.cognitive_tags = payload.get("cognitive_tags", [])
                existing.radar_scores = payload.get("radar_scores", {})
                existing.intervention_action = payload.get("intervention_action")
                action = "更新"
            else:
                session.add(
                    StudentProfile(
                        student_id=student_id,
                        basic_tags=payload.get("basic_tags", []),
                        behavior_tags=payload.get("behavior_tags", []),
                        cognitive_tags=payload.get("cognitive_tags", []),
                        radar_scores=payload.get("radar_scores", {}),
                        intervention_action=payload.get("intervention_action"),
                    )
                )
                action = "插入"

            session.commit()
            print(f"✅ 已{action}画像到数据库: {student_id}")
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
