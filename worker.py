import json
import os
import re

import psycopg2
from redis import Redis
from rq import Queue, Worker

STOPWORDS = {"the", "a", "an", "and", "or", "but", "if", "then", "of",
             "in", "on", "at", "to", "for", "with", "by", "is", "are",
             "was", "were", "be", "been", "being", "this", "that"}


def _get_db():
    return psycopg2.connect(os.environ["DATABASE_URL"])


def _update_job(job_id, **kwargs):
    conn = _get_db()
    try:
        cur = conn.cursor()
        sets = ", ".join(f"{k} = %s" for k in kwargs)
        vals = list(kwargs.values()) + [job_id]
        cur.execute(f"UPDATE jobs SET {sets}, updated_at = NOW() WHERE id = %s", vals)
        conn.commit()
    finally:
        conn.close()


def _enqueue_next(func, *args):
    r = Redis.from_url(os.environ["REDIS_URL"])
    q = Queue("pipeline", connection=r)
    q.enqueue(func, *args)


def run_stage1(job_id, text):
    try:
        _update_job(job_id, status="running", current_stage=1)
        path = f"/data/{job_id}"
        os.makedirs(path, exist_ok=True)
        with open(f"{path}/stage1.txt", "w") as f:
            f.write(text)
        _enqueue_next(run_stage2, job_id)
    except Exception as e:
        _update_job(job_id, status="failed", failed_stage=1, error=str(e))


def run_stage2(job_id):
    try:
        _update_job(job_id, status="running", current_stage=2)
        with open(f"/data/{job_id}/stage1.txt") as f:
            text = f.read()
        with open(f"/data/{job_id}/stage2.txt", "w") as f:
            f.write(text.lower())
        _enqueue_next(run_stage3, job_id)
    except Exception as e:
        _update_job(job_id, status="failed", failed_stage=2, error=str(e))


def run_stage3(job_id):
    try:
        _update_job(job_id, status="running", current_stage=3)
        with open(f"/data/{job_id}/stage2.txt") as f:
            text = f.read()
        tokens = re.findall(r"\w+", text)
        with open(f"/data/{job_id}/stage3.json", "w") as f:
            json.dump(tokens, f)
        _enqueue_next(run_stage4, job_id)
    except Exception as e:
        _update_job(job_id, status="failed", failed_stage=3, error=str(e))


def run_stage4(job_id):
    try:
        _update_job(job_id, status="running", current_stage=4)
        with open(f"/data/{job_id}/stage3.json") as f:
            tokens = json.load(f)
        filtered = [w for w in tokens if w not in STOPWORDS]
        with open(f"/data/{job_id}/stage4.json", "w") as f:
            json.dump(filtered, f)
        _enqueue_next(run_stage5, job_id)
    except Exception as e:
        _update_job(job_id, status="failed", failed_stage=4, error=str(e))


def run_stage5(job_id):
    try:
        _update_job(job_id, status="running", current_stage=5)
        with open(f"/data/{job_id}/stage4.json") as f:
            words = json.load(f)
        if not words:
            raise ValueError("No words remain after stopword removal")
        freq = {}
        for w in words:
            freq[w] = freq.get(w, 0) + 1
        with open(f"/data/{job_id}/stage5.json", "w") as f:
            json.dump(freq, f)
        top5 = sorted(freq.items(), key=lambda x: x[1], reverse=True)[:5]
        conn = _get_db()
        try:
            cur = conn.cursor()
            for word, count in top5:
                cur.execute(
                    "INSERT INTO top_words (job_id, word, count) VALUES (%s, %s, %s)",
                    (job_id, word, count),
                )
            conn.commit()
        finally:
            conn.close()
        _update_job(job_id, status="completed")
    except Exception as e:
        _update_job(job_id, status="failed", failed_stage=5, error=str(e))


if __name__ == "__main__":
    r = Redis.from_url(os.environ["REDIS_URL"])
    Worker(["pipeline"], connection=r).work()
