from typing import Dict, List

import jieba


class Preprocessor:
    def __init__(self):
        self.stop_words = {"的", "了", "和", "是", "就", "都", "而", "及", "与"}

    def clean_numeric(self, raw_data: Dict[str, float]) -> Dict[str, float]:
        return {key: (value if value is not None else 0.0) for key, value in raw_data.items()}

    def tokenize_texts(self, texts: List[str]) -> List[List[str]]:
        result: List[List[str]] = []
        for text in texts:
            tokens = jieba.lcut(text)
            result.append([t for t in tokens if t not in self.stop_words and len(t) > 1])
        return result
