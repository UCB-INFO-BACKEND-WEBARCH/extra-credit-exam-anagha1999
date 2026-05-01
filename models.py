from datetime import datetime
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class Job(db.Model):
    __tablename__ = "jobs"

    id = db.Column(db.String, primary_key=True)
    status = db.Column(db.String, nullable=False, default="pending")
    current_stage = db.Column(db.Integer, nullable=False, default=0)
    failed_stage = db.Column(db.Integer, nullable=True)
    error = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class TopWord(db.Model):
    __tablename__ = "top_words"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    job_id = db.Column(db.String, db.ForeignKey("jobs.id"), nullable=False)
    word = db.Column(db.Text, nullable=False)
    count = db.Column(db.Integer, nullable=False)
