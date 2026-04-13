from typing import List


class NlpService:
    """
    预留 BERTopic 接口，当前先返回 mock 主题。
    """

    def infer_topics(self, texts: List[str]) -> List[str]:
        if not texts:
            return []
        return ["模拟_考研压力", "模拟_科技前沿"]
