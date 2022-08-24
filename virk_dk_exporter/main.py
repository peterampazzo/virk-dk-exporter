from dataclasses import dataclass
from elasticsearch6 import Elasticsearch
from pyhocon import ConfigFactory, ConfigTree
import logging
import sys
import json
from typing import List, Dict, Any

logging.basicConfig(
    level=logging.getLevelName("INFO"),
    format="[%(asctime)s] - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)


def process_hits(hits):
    for item in hits:
        logging.debug(json.dumps(item, indent=2))


def search_by_cvr(cvr: str) -> List[Dict[str, Any]]:
    r = client.search(
        index="cvr-permanent", body={"query": {"term": {"Vrvirksomhed.cvrNummer": cvr}}}
    )
    process_hits(r["hits"]["hits"])
    return r["hits"]["hits"]


def query_with_scroll(timeout=1000, index="cvr-permanent", doc_type="type", size=1000):
    data = []
    count = 0
    body = {
        # "_source": [
        #     "Vrvirksomhed.virksomhedMetadata.nyesteNavn.navn",
        #     "Vrvirksomhed.virksomhedMetadata.nyesteBeliggenhedsadresse.landekode",
        # ],
        "query": {
            "query_string": {
                "default_field": "Vrvirksomhed.virksomhedMetadata.nyesteBeliggenhedsadresse.kommune.kommuneKode",
                "query": 169,
            }
        },
    }
    results = client.search(index=index, scroll="5m", size=size, body=body)
    sid = results["_scroll_id"]
    scroll_size = len(results["hits"]["hits"])
    # print(results["hits"]["hits"])

    while scroll_size > 0:
        logging.info(f"Page: {count}")
        logging.info(f"Fetched {len(results['hits']['hits'])} elements.")
        data.extend(results["hits"]["hits"])
        # Before scroll, process current batch of hits
        # process_hits(data["hits"]["hits"])

        results = client.scroll(scroll_id=sid, scroll="5m")

        # Update the scroll ID
        sid = results["_scroll_id"]

        # Get the number of results that returned in the last scroll
        scroll_size = len(results["hits"]["hits"])
        count += 1
    
    with open('json_data.json', 'w') as outfile:
        json.dump(data, outfile)


def conn(connection: ConfigTree) -> Elasticsearch:
    client = Elasticsearch(
        hosts=f"{connection.host}:{connection.port}",
        http_auth=(
            connection.user,
            connection.password,
        ),
    )

    Elasticsearch.info(client)
    return client


config = ConfigFactory.parse_file("app.conf")
client = conn(config["connection"])


def main():
    # search_by_cvr("11111112")
    query_with_scroll()

