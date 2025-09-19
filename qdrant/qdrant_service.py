import os

from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance
import uuid
import numpy as np
from hdbscan import HDBSCAN


class QdrantService:
    _client = None
    COLLECTION_NAME = "news_embedding"

    @classmethod
    def get_client(cls):
        if cls._client is None:
            cls._client = QdrantClient(
                host=os.getenv("QDRANT_HOST"),
                port=os.getenv("QDRANT_PORT"),
                timeout=60,
                api_key='123456',
                https=False,
            )
            cls._init_collection()
        return cls._client

    @classmethod
    def _init_collection(cls):
        client = cls.get_client()
        collections = client.get_collections().collections
        collection_names = [c.name for c in collections]

        if cls.COLLECTION_NAME not in collection_names:
            client.create_collection(
                collection_name=cls.COLLECTION_NAME,
                vectors_config=VectorParams(
                    size=768,
                    distance=Distance.DOT
                )
            )

    def insert_embeddings(self, embeddings, metadata_list=None):
        """插入嵌入向量到Qdrant"""
        client = self.get_client()
        points = []
        for idx, vector in enumerate( embeddings):
            # 自动生成唯一ID
            point_id = str(uuid.uuid4())

            payload = {
            }
            if metadata_list and idx < len(metadata_list):
                payload.update(metadata_list[idx])

            points.append(PointStruct(
                id=point_id,
                vector=vector,
                payload=payload
            ))

        return client.upsert(
            collection_name=self.COLLECTION_NAME,
            wait=True,
            points=points
        )

    def run_hdbscan_clustering(self, vectors, payloads, min_cluster_size=3, project_name=None):
        clusterer = HDBSCAN(min_cluster_size=2, min_samples=1, metric='euclidean')
        labels = clusterer.fit_predict(vectors)
        clustered = {}
        for label, payload in zip(labels, payloads):
            clustered.setdefault(label, []).append(payload)
        
        return clustered

        # print(f"\n=== Project: {project_name} ===")
        # for cluster_id, items in clustered.items():
        #     print(f"\n🧠 Cluster {cluster_id} ({len(items)} items):")
        #     for item in items[:5]:  # 每簇最多展示前5个
        #         print(f"  - {item.get('source_name')} | id: {item.get('source_id')} | db: {item.get('source_db')}")

        # print(f"\n✅ 总共发现 {len(set(labels)) - (1 if -1 in labels else 0)} 个簇，{np.sum(labels == -1)} 条为噪声（cluster -1）")


    def fetch_vectors_by_payloads(self, search_conditions, collection_name=None):
        """
        根据payload中的source_name和source_id批量检索向量。
        search_conditions: List[Dict], 例如 [{"source_name": ..., "source_id": ...}, ...]
        返回: vectors(np.ndarray), payloads(list)
        """
        client = self.get_client()
        collection = collection_name or self.COLLECTION_NAME
        all_vectors = []
        all_payloads = []
        for cond in search_conditions:
            # 构造payload过滤条件
            query = {
                "must": [
                    {"key": "source_name", "match": {"value": cond["source_name"]}},
                    {"key": "source_id", "match": {"value": cond["source_id"]}}
                ]
            }
            result = client.scroll(
                collection_name=collection,
                limit=10,
                with_payload=True,
                with_vectors=True,
                scroll_filter=query
            )[0]
            for point in result:
                all_vectors.append(point.vector)
                all_payloads.append(point.payload)
        if all_vectors:
            return np.array(all_vectors), all_payloads
        else:
            return [], []