import os
from typing import Set
from cachetools.func import ttl_cache
from atproto import models
from azure.data.tables import TableServiceClient

from server.database import Post, db
from server.logger import logger


class PostClassifier:
    def __init__(self):
        connection_string = os.environ["CONNECTION_STRING"]
        self.service = TableServiceClient.from_connection_string(connection_string)
        self.users_client = self.service.create_table_if_not_exists(table_name="users")

    @ttl_cache(ttl=300)
    def tracked_dids(self) -> Set[str]:
        return set([e["DID"] for e in self.users_client.list_entities()])

    def callback(self, ops: dict) -> None:
        posts_to_create = []
        if ops["posts"]["created"]:
            # Get the tracked user DIDs
            tracked_dids = self.tracked_dids()

            # Here we can filter, process, run ML classification, etc.
            # After our feed alg we can save posts into our DB
            # Also, we should process deleted posts to remove them from our DB and keep it in sync
            for created_post in ops["posts"]["created"]:
                record = created_post["record"]

                # Check to see if the author is in the set of tracked DIDs
                if created_post["author"] in tracked_dids:
                    logger.info("Adding post: %s", record)

                    reply_parent = None
                    if record.reply and record.reply.parent.uri:
                        reply_parent = record.reply.parent.uri

                    reply_root = None
                    if record.reply and record.reply.root.uri:
                        reply_root = record.reply.root.uri

                    post_dict = {
                        "uri": created_post["uri"],
                        "cid": created_post["cid"],
                        "reply_parent": reply_parent,
                        "reply_root": reply_root,
                    }
                    posts_to_create.append(post_dict)

        posts_to_delete = [p["uri"] for p in ops["posts"]["deleted"]]
        if posts_to_delete:
            Post.delete().where(Post.uri.in_(posts_to_delete))
            logger.debug(f"Deleted from feed: {len(posts_to_delete)}")

        if posts_to_create:
            with db.atomic():
                for post_dict in posts_to_create:
                    Post.create(**post_dict)
            logger.info(f"Added to feed: {len(posts_to_create)}")


pc = PostClassifier()
