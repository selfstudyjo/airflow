#!/bin/bash
set -e

export AIRFLOW_HOME=/opt/airflow
export PATH="/home/airflow/.local/bin:${PATH}"

echo "========================================"
echo "  Starting Airflow on Fly.io"
echo "========================================"

# ─── Configure Airflow ───
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__SQL_ALCHEMY_CONN="sqlite:////opt/airflow/airflow.db"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
export AIRFLOW__CORE__PARALLELISM=2
export AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=1
export AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=1
export AIRFLOW__CORE__DAG_FILE_PROCESSOR_TIMEOUT=120
export AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=120
export AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=120
export AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=600
export AIRFLOW__SCHEDULER__PARSING_PROCESSES=1
export AIRFLOW__WEBSERVER__WORKERS=1
export AIRFLOW__WEBSERVER__WORKER_REFRESH_INTERVAL=9000
export AIRFLOW__WEBSERVER__WEB_SERVER_WORKER_TIMEOUT=300
export AIRFLOW__WEBSERVER__BASE_URL=https://airflow-i6nkzw.fly.dev
export AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session

# ─── Initialize Database ───
echo "[init] Migrating database..."
airflow db migrate 2>&1 | tail -3

# ─── Create admin user ───
echo "[init] Creating admin user..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin 2>/dev/null || echo "[init] Admin already exists"

# ─── Set Variables ───
if [ -n "$AUTH_TOKEN" ]; then
    airflow variables set AUTH_TOKEN "$AUTH_TOKEN" 2>/dev/null || true
    echo "[init] AUTH_TOKEN set"
else
    echo "[init] WARNING: AUTH_TOKEN not configured"
    echo "[init] Set it later: fly secrets set AUTH_TOKEN=your_token"
fi

# ─── Create Pools ───
airflow pools set reload_apps_pool 3 "Selenium extend tasks" 2>/dev/null || true
airflow pools set ping_apps_pool 5 "HTTP ping tasks" 2>/dev/null || true
echo "[init] Pools created"

# ─── DAG sync from GitHub (background) ───
(
    while true; do
        sleep 300
        REPO_DIR="/tmp/dags-repo"
        if [ -d "$REPO_DIR/.git" ]; then
            cd "$REPO_DIR"
            git fetch origin 2>/dev/null
            git reset --hard origin/main 2>/dev/null || git reset --hard origin/master 2>/dev/null
        else
            git clone https://github.com/selfstudyjo/airflow.git "$REPO_DIR" 2>/dev/null
        fi
        [ -d "$REPO_DIR/dags" ] && cp "$REPO_DIR"/dags/*.py /opt/airflow/dags/ 2>/dev/null
    done
) &

# ─── Log cleanup (background) ───
(
    while true; do
        sleep 3600
        find /opt/airflow/logs -type f -mtime +1 -delete 2>/dev/null
        find /opt/airflow/logs -type d -empty -delete 2>/dev/null
    done
) &

# ─── Start Scheduler (background) ───
echo "[init] Starting scheduler..."
airflow scheduler &

sleep 5

# ─── Start Webserver (foreground) ───
echo ""
echo "========================================="
echo "  Airflow is ready!"
echo "  Username: admin"
echo "  Password: admin"
echo "========================================="
echo ""

exec airflow webserver --port 8080