from atproto import models

from server.database import Post, db
from server.logger import logger


FRAGMENTS_TO_FIND = ["magic: the gathering", "magicthegathering"]


def operations_callback(ops: dict) -> None:
    # Here we can filter, process, run ML classification, etc.
    # After our feed alg we can save posts into our DB
    # Also, we should process deleted posts to remove them from our DB and keep it in sync

    # for example, let's create our custom feed that will contain all posts that contains alf related text

    posts_to_create = []
    for created_post in ops["posts"]["created"]:
        record = created_post["record"]

        is_magic = any([f in record.text.lower() for f in FRAGMENTS_TO_FIND])

        # only magic-related posts
        if is_magic:
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
