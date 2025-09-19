from sentence_transformers import SentenceTransformer


class EmbeddingService:
    _model = None

    def __init__(self):
        if not EmbeddingService._model:
            EmbeddingService._model = SentenceTransformer('thenlper/gte-base', device="cpu")

    def generate_embeddings(self, sentences):
        embeddings = self._model.encode(["测试句子"], convert_to_numpy=True)
        print(embeddings.shape)

        return self._model.encode(sentences, convert_to_numpy=True).tolist()