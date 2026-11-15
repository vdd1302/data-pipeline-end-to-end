"""A3.1 local single-user Superset TEST metadata. Never production settings."""
import os
SECRET_KEY = os.environ['SUPERSET_SECRET_KEY']
SQLALCHEMY_DATABASE_URI = 'sqlite:////app/superset_home/superset.db'
WTF_CSRF_ENABLED = True
# Local HTTP-only Docker Desktop test; no public bind, proxy or anonymous access.
ENABLE_PROXY_FIX = False
ROW_LIMIT = 1000
SQLLAB_ASYNC_TIME_LIMIT_SEC = 10
