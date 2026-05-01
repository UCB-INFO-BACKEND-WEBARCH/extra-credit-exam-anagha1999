"""Pipeline service — student stub.

You are responsible for implementing every endpoint in the spec.
The starter only sets up an empty Flask app and the SQLAlchemy binding.
"""

import os
from flask import Flask

from models import db


def create_app():
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = os.environ["DATABASE_URL"]
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    db.init_app(app)

    with app.app_context():
        db.create_all()

    # TODO: register routes (POST /jobs, GET /jobs/<id>, GET /health)
    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
