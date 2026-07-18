#!/bin/bash
set -e

export AIRFLOW_HOME=/opt/airflow

echo "========================================"
echo "  Starting Airflow on Fly.io"
echo "========================================"

# ─── Sync DAGs from GitHub ───
echo "[init] Syncing DAGs from GitHub..."
REPO_DIR="/tmp/dags-repo"
if [ -d "$REPO_DIR/.git" ]; then
    cd "$REPO_DIR"
    git fetch origin 2>/dev/null
    git reset --hard origin/main 2>/dev/null || git reset --hard origin/master 2>/dev/null
else
    git clone https://github.com/selfstudyjo/airflow.git "$REPO_DIR" 2>/dev/null || true
fi
if [ -d "$REPO_DIR/dags" ]; then
    cp "$REPO_DIR"/dags/*.py /opt/airflow/dags/ 2>/dev/null || true
fi
echo "[init] DAGs in /opt/airflow/dags/:"
ls -la /opt/airflow/dags/*.py 2>/dev/null || echo "  No DAG files found"

# ─── Configure Airflow via environment ───
echo "[init] Configuring Airflow..."

export AIRFLOW__CORE__EXECUTOR=LocalExecutor
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
export AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth

# ─── Use DATABASE_URL if set, otherwise SQLite ───
if [ -n "$DATABASE_URL" ]; then
    export AIRFLOW__CORE__SQL_ALCHEMY_CONN="$DATABASE_URL"
    echo "[init] Using PostgreSQL database"
else
    export AIRFLOW__CORE__SQL_ALCHEMY_CONN="sqlite:////opt/airflow/airflow.db"
    echo "[init] Using SQLite database"
fi

# ─── Initialize Database ───
echo "[init] Migrating database..."
airflow db migrate 2>&1 | tail -5

# ─── Create admin user ───
echo "[init] Creating admin user..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin 2>/dev/null || echo "[init] Admin user already exists"

# ─── Set Variables ───
echo "[init] Setting variables and pools..."
if [ -n "$AUTH_TOKEN" ]; then
    airflow variables set AUTH_TOKEN "$AUTH_TOKEN" 2>/dev/null || true
    echo "[init] AUTH_TOKEN variable set"
else
    echo "[init] WARNING: AUTH_TOKEN not set - set it as a Fly secret"
fi

# ─── Create Pools ───
airflow pools set reload_apps_pool 3 "Selenium extend tasks" 2>/dev/null || true
airflow pools set ping_apps_pool 5 "HTTP ping tasks" 2>/dev/null || true
echo "[init] Pools created"

# ─── DAG sync background job ───
echo "[init] Starting background DAG sync (every 5 min)..."
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
        if [ -d "$REPO_DIR/dags" ]; then
            cp "$REPO_DIR"/dags/*.py /opt/airflow/dags/ 2>/dev/null
        fi
    done
) &

# ─── Log cleanup background job ───
(
    while true; do
        sleep 3600
        find /opt/airflow/logs -type f -mtime +1 -delete 2>/dev/null
        find /opt/airflow/logs -type d -empty -delete 2>/dev/null
    done
) &

# ─── Start Scheduler in background ───
echo "[init] Starting scheduler..."
airflow scheduler &
SCHEDULER_PID=$!

# Wait for scheduler to initialize
sleep 10

# ─── Start Webserver (foreground) ───
echo ""
echo "╔══════════════════════════════════════════╗"
echo "║   Airflow is ready!                      ║"
echo "║   Username: admin                        ║"
echo "║   Password: admin                        ║"
echo "╚══════════════════════════════════════════╝"
echo ""

exec airflow webserver --port 8080