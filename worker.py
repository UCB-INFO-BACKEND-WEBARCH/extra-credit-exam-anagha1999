"""rq worker entrypoint — student stub.

When this container starts, it should connect to Redis and process jobs from
the pipeline queue. Implement stage functions in this file (or a module
imported here) and the queue chaining strategy that runs them sequentially.
"""

import os

from redis import Redis
from rq import Queue, Worker

if __name__ == "__main__":
    redis_conn = Redis.from_url(os.environ["REDIS_URL"])
    # TODO: replace ["default"] with whatever queue name you use
    Worker(["default"], connection=redis_conn).work()
