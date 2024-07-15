# Elastic search implementation module

from elasticsearch import Elasticsearch
from elastic_transport import ObjectApiResponse
from elasticsearch.helpers import bulk
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
import os

load_dotenv(Path('.env'))
ELASTIC_SEARCH_SERVER_ADDRESS = os.getenv("ELASTIC_SEARCH_SERVER_ADDRESS")
PATH_TO_DATASET = os.getenv("PATH_TO_DATASET")

es = Elasticsearch(ELASTIC_SEARCH_SERVER_ADDRESS)
df = pd.read_csv(PATH_TO_DATASET).dropna().sample(3000, random_state=42).reset_index()

# Run the Elastic search cluster:
# docker run --rm -p 9200:9200 -p 9300:9300 -e "xpack.security.enabled=false" -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:8.14.0


class ElasticSearch:
    def __init__(self) -> None:
        self.__description = "Elastic Search code implementation."
        self.df = df

    @staticmethod
    def create_index(index_name: str, index_mapping: dict) -> ObjectApiResponse:
        index = es.indices.create(index=index_name, mappings=index_mapping)
        print(type(index))
        return index

    def add_data_to_index(self, index: str) -> None:
        for i, row in self.df.iterrows():
            doc = {
                "title": row["Title"],
                "ethnicity": row["Origin/Ethnicity"],
                "director": row["Director"],
                "cast": row["Cast"],
                "genre": row["Genre"],
                "plot": row["Plot"],
                "year": row["Release Year"],
                "wiki_page": row["Wiki Page"]
            }
            es.index(index=index, id=i, document=doc)
        # Refresh index
        es.indices.refresh(index=index)
        return

    def add_bulk_data_to_index(self, index: str) -> None:
        bulk_data = []
        for i, row in self.df.iterrows():
            bulk_data.append(
                {
                    "_index": "movies",
                    "_id": i,
                    "_source": {
                        "title": row["Title"],
                        "ethnicity": row["Origin/Ethnicity"],
                        "director": row["Director"],
                        "cast": row["Cast"],
                        "genre": row["Genre"],
                        "plot": row["Plot"],
                        "year": row["Release Year"],
                        "wiki_page": row["Wiki Page"],
                    }
                }
            )
        bulk(es, bulk_data)
        # Refresh index
        es.indices.refresh(index=index)
        return

    @staticmethod
    def delete_index(index: str):
        es.indices.delete(index=index)

    @staticmethod
    def delete_documents_from_index(index: str, _id: str):
        es.delete(index=index, id=_id)

    @staticmethod
    def get_all_indices() -> list:
        # USE REST API: http://localhost:9200/_cat/indices
        # print(es.indices.get_alias("movies"))
        return []

    @staticmethod
    def display_count_in_index(index: str):
        print("Count in index: {}".format(es.cat.count(index=index, format="json")))

    @staticmethod
    def search_data(index: str, query: dict):
        response = es.search(index=index, query=query)
        print(response.body)


elastic_search_obj = ElasticSearch()

mappings = {
        "properties": {
            "title": {"type": "text", "analyzer": "english"},
            "ethnicity": {"type": "text", "analyzer": "standard"},
            "director": {"type": "text", "analyzer": "standard"},
            "cast": {"type": "text", "analyzer": "standard"},
            "genre": {"type": "text", "analyzer": "standard"},
            "plot": {"type": "text", "analyzer": "english"},
            "year": {"type": "integer"},
            "wiki_page": {"type": "keyword"}
        }
}

# elastic_search_obj.create_index(index_name="movies", index_mapping=mappings)
# elastic_search_obj.add_data_to_index(index="movies")
elastic_search_obj.display_count_in_index(index="movies")

query = {
        "bool": {
            "must": {
                "match_phrase": {
                    "cast": "jack nicholson",
                }
            },
            "filter": {"bool": {"must_not": {"match_phrase": {"director": "roman polanski"}}}},
        },
}

elastic_search_obj.search_data("movies", query)



