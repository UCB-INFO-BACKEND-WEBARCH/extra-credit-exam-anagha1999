import os
import uuid

from flask import Flask, jsonify, request
from redis import Redis
from rq import Queue
from sqlalchemy import text

from models import Job, db


def create_app():
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = os.environ["DATABASE_URL"]
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    db.init_app(app)

    with app.app_context():
        db.create_all()

    @app.route("/health")
    def health():
        db_status = "up"
        redis_status = "up"
        volume_writable = False

        try:
            db.session.execute(text("SELECT 1"))
        except Exception:
            db_status = "down"

        try:
            r = Redis.from_url(os.environ["REDIS_URL"])
            r.ping()
        except Exception:
            redis_status = "down"

        try:
            test_path = "/data/.health_check"
            with open(test_path, "w") as f:
                f.write("ok")
            os.remove(test_path)
            volume_writable = True
        except Exception:
            volume_writable = False

        return jsonify({
            "status": "ok",
            "db": db_status,
            "redis": redis_status,
            "volume_writable": volume_writable,
        })

    @app.route("/jobs", methods=["POST"])
    def create_job():
        data = request.get_json()
        text_input = data.get("text", "")

        job_id = str(uuid.uuid4())
        job = Job(id=job_id, status="pending", current_stage=0)
        db.session.add(job)
        db.session.commit()

        r = Redis.from_url(os.environ["REDIS_URL"])
        q = Queue("pipeline", connection=r)
        q.enqueue("worker.run_stage1", job_id, text_input)

        return jsonify({"job_id": job_id}), 202

    @app.route("/jobs/<job_id>")
    def get_job(job_id):
        job = db.session.get(Job, job_id)
        if job is None:
            return jsonify({"error": "not found"}), 404
        return jsonify({
            "job_id": job.id,
            "status": job.status,
            "current_stage": job.current_stage,
            "failed_stage": job.failed_stage,
            "error": job.error,
        })

    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, threaded=True)
