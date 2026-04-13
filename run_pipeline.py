import datetime

from database.profile_repository import ProfileRepository
from services.clustering_service import ClusteringService
from services.nlp_service import NlpService
from services.preprocessor import Preprocessor


def main():
    print("🚀 开始执行画像构建流水线...")

    mock_raw_data = {
        "student_id": "STU_2023001",
        "structured_data": {"math_score": 85.5, "login_count": 25},
        "unstructured_texts": ["考研数学复习毫无头绪怎么办", "中国人工智能发展史太震撼了"],
    }

    preprocessor = Preprocessor()
    clustering = ClusteringService()
    nlp = NlpService()

    cleaned_num = preprocessor.clean_numeric(mock_raw_data["structured_data"])
    tokens = preprocessor.tokenize_texts(mock_raw_data["unstructured_texts"])
    joined_texts = [" ".join(t) for t in tokens]

    behavior_tags = clustering.infer_behavior_tags(cleaned_num)
    cognitive_tags = nlp.infer_topics(joined_texts)

    final_profile = {
        "student_id": mock_raw_data["student_id"],
        "basic_tags": ["理工类", "大三"],
        "behavior_tags": behavior_tags,
        "cognitive_tags": cognitive_tags,
        "radar_scores": {"updated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
        "intervention_action": None,
    }

    repository = ProfileRepository()
    repository.save_profile(final_profile)
    repository.close()
    print("🎉 流水线执行完毕！")


if __name__ == "__main__":
    main()
