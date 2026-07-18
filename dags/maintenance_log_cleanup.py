"""
Daily cleanup: delete Airflow task logs and scheduler/dag-processor logs
older than 2 days. Keeps the airflow-logs volume small forever.
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

RETENTION_DAYS = 2
LOG_ROOT = "/opt/airflow/logs"

default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="maintenance_log_cleanup",
    description=f"Delete Airflow log files older than {RETENTION_DAYS} days",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["maintenance"],
) as dag:

    cleanup = BashOperator(
        task_id="cleanup_old_logs",
        bash_command=f"""
set -e
echo "[cleanup] Target dir : {LOG_ROOT}"
echo "[cleanup] Retention  : {RETENTION_DAYS} day(s)"
echo "[cleanup] Size before: $(du -sh {LOG_ROOT} 2>/dev/null | cut -f1)"

# Delete log files older than RETENTION_DAYS days
find {LOG_ROOT} -type f -name '*.log' -mtime +{RETENTION_DAYS} -print -delete || true

# Delete *any* file older than RETENTION_DAYS (in case of rotated files etc.)
find {LOG_ROOT} -type f -mtime +{RETENTION_DAYS} -delete || true

# Remove empty leftover dirs
find {LOG_ROOT} -mindepth 1 -type d -empty -delete || true

echo "[cleanup] Size after : $(du -sh {LOG_ROOT} 2>/dev/null | cut -f1)"
echo "[cleanup] Done."
""",
    )