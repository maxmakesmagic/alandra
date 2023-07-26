import argparse
import logging
import os
import sys
from typing import Dict
from azure.data.tables import TableServiceClient

from atproto import Client, models

log = logging.getLogger(__name__)


def environ_or_required(key: str) -> Dict[str, str]:
    if key in os.environ:
        return {"default": os.environ[key]}
    else:
        return {"required": True}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bsky-username", **environ_or_required("BSKY_USERNAME"))
    parser.add_argument("--bsky-password", **environ_or_required("BSKY_PASSWORD"))
    parser.add_argument(
        "--connection-string", **environ_or_required("CONNECTION_STRING")
    )
    parser.add_argument(
        "-u",
        "--users_to_track",
        nargs="*",
        help="The user handles to track",
        default=[],
    )
    args = parser.parse_args()

    client = Client()
    client.login(args.bsky_username, args.bsky_password)

    # Create a client to access the database.
    service = TableServiceClient.from_connection_string(args.connection_string)
    table_name = "users"
    table_client = service.create_table_if_not_exists(table_name=table_name)

    for handle in args.users_to_track:
        log.info("Handle to track: %s", handle)
        params = models.ComAtprotoIdentityResolveHandle.Params(handle)
        response = client.com.atproto.identity.resolve_handle(params)
        entity = {"PartitionKey": "alandra", "RowKey": handle, "DID": response.did}

        # Insert the tracked user in the table store
        table_client.upsert_entity(entity)
        log.info("Inserted entity: %s", entity)

    # List all of the entities
    for e in table_client.list_entities(select=["RowKey", "DID"]):
        log.info("Tracked: %s (%s)", e["RowKey"], e["DID"])


def run():
    formatter = logging.Formatter("%(asctime)s %(message)s")
    streamhandler = logging.StreamHandler(stream=sys.stdout)
    streamhandler.setFormatter(formatter)
    streamhandler.setLevel(logging.DEBUG)

    root_logger = logging.getLogger()
    root_logger.addHandler(streamhandler)
    root_logger.setLevel(logging.DEBUG)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("msal").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    main()


if __name__ == "__main__":
    run()
