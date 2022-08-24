from elasticsearch6 import Elasticsearch
from pyhocon import ConfigFactory, ConfigTree
import logging
import sys
import json
from typing import List, Dict, Any
import pandas as pd
import argparse

logging.basicConfig(
    level=logging.getLevelName("INFO"),
    format="[%(asctime)s] - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)


# def process_hits(hits):
#     for item in hits:
#         logging.debug(json.dumps(item, indent=2))


def search_by_cvr(cvr: str) -> List[Dict[str, Any]]:
    res = client.search(
        index="cvr-permanent", body={"query": {"term": {"Vrvirksomhed.cvrNummer": cvr}}}
    )
    return res["hits"]["hits"]


def query_with_scroll(
    field,
    value,
    timeout=1000,
    index="cvr-permanent",
    doc_type="type",
    size=1000,
    scroll="5m",
):
    data = []
    count = 0
    body = {
        "query": {
            "query_string": {
                "default_field": field,
                "query": value,
            }
        },
    }
    results = client.search(index=index, scroll=scroll, size=size, body=body)
    sid = results["_scroll_id"]
    scroll_size = len(results["hits"]["hits"])

    while scroll_size > 0:
        logging.info(f"Page: {count}")
        logging.info(f"Fetched {len(results['hits']['hits'])} elements.")
        data.extend(results["hits"]["hits"])
        # Before scroll, process current batch of hits
        # process_hits(data["hits"]["hits"])

        results = client.scroll(scroll_id=sid, scroll=scroll)

        # Update the scroll ID
        sid = results["_scroll_id"]

        # Get the number of results that returned in the last scroll
        scroll_size = len(results["hits"]["hits"])
        count += 1

    return data


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
    parser = argparse.ArgumentParser(description="Export data from Virk.dk.")
    parser.add_argument(
        "profile", metavar="profile", type=str, help="an integer for the accumulator"
    )
    parser.add_argument(
        "value", metavar="value", type=str, help="an integer for the accumulator"
    )
    args = parser.parse_args()

    if args.profile == "cvr":
        res = search_by_cvr(args.value)
    elif args.profile == "municipality":
        res = query_with_scroll(
            field="Vrvirksomhed.virksomhedMetadata.nyesteBeliggenhedsadresse.kommune.kommuneKode",
            value=args.value,
        )
    pd.DataFrame(res).to_csv("test.csv", index=False)
