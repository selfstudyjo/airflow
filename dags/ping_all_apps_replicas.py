"""
Hourly ping of /metrics/ on every replica of every app.
One DAG per app – mirrors the structure of extend_all_apps.py.

For every replica we:
  - GET <replica_url>/metrics/  with header "Authorization: Token <AUTH_TOKEN>"
  - log status code
  - log the JSON body returned
"""

from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import json
import requests

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
MAIN_SERVER_URL = "https://sfsdomains1.pythonanywhere.com"
DOMAINS = [
    "https://sfsdomains1.pythonanywhere.com",
    "https://sfsdomains2.pythonanywhere.com",
]

# Separate pool for ping tasks – keep load on replicas under control.
PING_POOL = "ping_apps_pool"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def get_apps_from_server():
    """Fetch all apps from the main server (DAG-parse time)."""
    try:
        auth_token = Variable.get("AUTH_TOKEN")
    except Exception as e:
        logger.error(f"AUTH_TOKEN variable not set: {e}")
        return []

    headers = {
        "Authorization": f"Token {auth_token}",
        "Content-Type": "application/json",
    }
    url = f"{MAIN_SERVER_URL}/apps/"
    try:
        logger.info(f"Fetching apps from {url}")
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        return [{"id": a["id"], "name": a["app_name"]} for a in resp.json()]
    except Exception as e:
        logger.error(f"Failed to fetch apps: {e}")
        return []


def sanitize(name: str) -> str:
    return (
        name.replace(" ", "_")
            .replace("-", "_")
            .replace("(", "")
            .replace(")", "")
            .replace(".", "_")
            .replace("/", "_")
    )


# ------------------------------------------------------------
# Tasks
# ------------------------------------------------------------
@task
def fetch_replicas(app_id: int):
    """
    Returns a list of dicts: {"username": ..., "url": "https://<user>.pythonanywhere.com"}.
    Tries every domain in DOMAINS until one succeeds.
    """
    auth_token = Variable.get("AUTH_TOKEN")
    headers = {
        "Authorization": f"Token {auth_token}",
        "Content-Type": "application/json",
    }
    for domain in DOMAINS:
        url = f"{domain}/apps/{app_id}"
        try:
            logger.info(f"Fetching from {url}")
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            replicas = []
            for r in data.get("replicas", []):
                username = r["replica_username"]
                # Prefer an explicit URL if the API ever returns one,
                # otherwise build the standard PythonAnywhere URL.
                replica_url = (
                    r.get("replica_url")
                    or r.get("url")
                    or f"https://{username}.pythonanywhere.com"
                )
                replicas.append({
                    "username": username,
                    "url": replica_url.rstrip("/"),
                })
            logger.info(f"App {app_id}: {len(replicas)} replicas")
            return replicas
        except Exception as e:
            logger.warning(f"Failed to fetch from {url}: {e}")
            continue
    logger.error(f"No data for app {app_id}")
    return []


@task(pool=PING_POOL, pool_slots=1, retries=1, retry_delay=timedelta(minutes=1))
def ping_replica(replica: dict, app_name: str):
    """
    GET <replica_url>/metrics/ with the AUTH_TOKEN header
    and print the JSON body to logs.
    """
    auth_token = Variable.get("AUTH_TOKEN")
    headers = {
        "Authorization": f"Token {auth_token}",
        "Accept": "application/json",
        "User-Agent": "airflow-metrics-ping/1.0",
    }

    username = replica["username"]
    metrics_url = f"{replica['url']}/metrics/"
    logger.info(f"[{app_name}] [{username}] GET {metrics_url}")

    try:
        resp = requests.get(metrics_url, headers=headers, timeout=30)
    except requests.RequestException as e:
        logger.error(f"[{app_name}] [{username}] request failed: {e}")
        raise

    logger.info(
        f"[{app_name}] [{username}] HTTP {resp.status_code} "
        f"(elapsed {resp.elapsed.total_seconds():.2f}s)"
    )

    # Try to parse JSON; fall back to raw text
    try:
        body = resp.json()
        logger.info(
            f"[{app_name}] [{username}] METRICS:\n"
            + json.dumps(body, indent=2, sort_keys=True)
        )
    except ValueError:
        body = resp.text
        logger.warning(
            f"[{app_name}] [{username}] response was not JSON. Body (first 500 chars):\n"
            f"{body[:500]}"
        )

    if resp.status_code != 200:
        raise Exception(
            f"[{app_name}] [{username}] non-200 status: {resp.status_code}"
        )

    return {
        "username": username,
        "status_code": resp.status_code,
        "metrics": body if isinstance(body, dict) else None,
    }


# ------------------------------------------------------------
# DAG factory
# ------------------------------------------------------------
def build_dag(app_id: int, app_name: str) -> DAG:
    safe = sanitize(app_name)
    dag_id = f"ping_app_{app_id}_{safe}"

    default_args = {
        "owner": "admin",
        "depends_on_past": False,
        "start_date": datetime(2025, 1, 1),
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    }

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f"Hourly /metrics/ ping: {app_name}",
        schedule_interval="@hourly",
        catchup=False,
        tags=["pythonanywhere", "ping_app", "metrics"],
        max_active_runs=1,
    ) as dag:
        replicas = fetch_replicas(app_id)
        pings = ping_replica.partial(app_name=app_name).expand(replica=replicas)
        done = DummyOperator(task_id="all_pings_done")
        pings >> done

    return dag


# ------------------------------------------------------------
# Generate one DAG per app
# ------------------------------------------------------------
APPS = get_apps_from_server()
logger.info(f"DAG parse: generating {len(APPS)} ping DAGs")

for _app in APPS:
    _dag = build_dag(_app["id"], _app["name"])
    globals()[_dag.dag_id] = _dag