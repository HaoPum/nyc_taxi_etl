# superset/superset_config.py

# Explicitly set the database URI.
# This is more reliable than relying on environment variables that might be
# handled differently by various startup scripts.
SQLALCHEMY_DATABASE_URI = "postgresql://airflow:airflow@postgres-airflow/superset"

# A secret key for signing session cookies.
SECRET_KEY = "your_strong_secret_key_here"

# Enable template processing for SQL Lab to use Jinja templating in queries
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}