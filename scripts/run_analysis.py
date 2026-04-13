import datetime
import os
import sys
from typing import Any, Dict

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

from database.profile_repository import ProfileRepository
from services.clustering_service import ClusteringService
from services.nlp_service import NlpService
from services.preprocessor import Preprocessor


def run_once(student_record: Dict[str, Any]) -> None:
    preprocessor = Preprocessor()
    clustering = ClusteringService()
    nlp = NlpService()
    repository = ProfileRepository()

    cleaned_num = preprocessor.clean_numeric(student_record["structured_data"])
    tokenized = preprocessor.tokenize_texts(student_record["unstructured_texts"])
    joined_texts = [" ".join(tokens) for tokens in tokenized]

    behavior_tags = clustering.infer_behavior_tags(cleaned_num)
    cognitive_tags = nlp.infer_topics(joined_texts)

    final_profile = {
        "student_id": student_record["student_id"],
        "basic_tags": ["理工类", "大三"],
        "behavior_tags": behavior_tags,
        "cognitive_tags": cognitive_tags,
        "radar_scores": {"updated_at": datetime.datetime.now().isoformat()},
        "intervention_action": None,
    }

    repository.save_profile(final_profile)
    repository.close()


if __name__ == "__main__":
    mock_student = {
        "student_id": "STU_demo_001",
        "structured_data": {"math_score": 85.5, "login_count": 25},
        "unstructured_texts": ["考研数学复习毫无头绪怎么办", "中国人工智能发展史太震撼了"],
    }
    run_once(mock_student)
    print("🎉 分析与入库流程执行完成")
