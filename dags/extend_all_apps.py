"""
Dynamic DAG generator: one DAG per PythonAnywhere app.
All extend_replica tasks share a global pool (reload_apps_pool, 3 slots),
so only 3 tasks across ALL these DAGs run simultaneously.
"""

from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import requests

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
MAIN_SERVER_URL = "https://sfsdomains1.pythonanywhere.com"
DOMAINS = [
    "https://sfsdomains1.pythonanywhere.com",
    "https://sfsdomains2.pythonanywhere.com",
]

GLOBAL_POOL = "reload_apps_pool"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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


@task
def fetch_replicas(app_id: int):
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
            replicas = [
                {"username": r["replica_username"],
                 "password": r["replica_password"]}
                for r in data.get("replicas", [])
            ]
            logger.info(f"App {app_id}: {len(replicas)} replicas")
            return replicas
        except Exception as e:
            logger.warning(f"Failed to fetch from {url}: {e}")
            continue
    logger.error(f"No data for app {app_id}")
    return []


@task(pool=GLOBAL_POOL, pool_slots=1)
def extend_replica(replica: dict, app_name: str):
    import time
    from selenium import webdriver
    from selenium.webdriver.firefox.service import Service as FirefoxService
    from selenium.webdriver.firefox.options import Options as FirefoxOptions
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException

    username = replica["username"]
    password = replica["password"]
    logger.info(f"Starting extension for {username} (app: {app_name})")

    opts = FirefoxOptions()
    opts.add_argument("--headless")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.set_preference(
        "general.useragent.override",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0",
    )

    service = FirefoxService(executable_path="/usr/local/bin/geckodriver")
    driver = webdriver.Firefox(service=service, options=opts)
    driver.set_window_size(1920, 1080)

    try:
        driver.delete_all_cookies()
        driver.get("https://www.pythonanywhere.com/login/?next=/")

        u = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "id_auth-username"))
        )
        u.clear(); u.send_keys(username)

        p = driver.find_element(By.ID, "id_auth-password")
        p.clear(); p.send_keys(password)

        WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable(
                (By.XPATH, '//button[contains(text(), "Log in")]')
            )
        ).click()

        WebDriverWait(driver, 20).until(
            EC.any_of(
                EC.presence_of_element_located(
                    (By.XPATH, f'//a[contains(@href, "/user/{username}/")]')),
                EC.url_contains("/user/"),
            )
        )
        logger.info("Login OK")

        driver.get(f"https://www.pythonanywhere.com/user/{username}/webapps/")
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

        if "You haven't created any web apps" in driver.page_source:
            return {"status": "no_webapps", "username": username}

        selectors = [
            '//input[@type="submit" and contains(@value, "Run until")]',
            '//button[contains(text(), "Run until")]',
            '//input[contains(@value, "Run until")]',
            '//input[@type="submit" and contains(@value, "Extend")]',
            '//button[contains(text(), "Extend")]',
        ]
        btn = None
        for sel in selectors:
            try:
                btn = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, sel))
                )
                break
            except TimeoutException:
                continue

        if not btn:
            if "expires on" in driver.page_source:
                return {"status": "not_due", "username": username}
            raise Exception(f"Extend button not found for {username}")

        driver.execute_script("arguments[0].scrollIntoView(true);", btn)
        time.sleep(1)
        btn.click()
        logger.info("Extend button clicked")

        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located(
                    (By.XPATH, '//div[contains(@class, "alert-success")]'))
            )
        except TimeoutException:
            time.sleep(5)

        return {"status": "extended", "username": username}

    except Exception as e:
        logger.error(f"Error for {username}: {e}")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        try:
            driver.save_screenshot(f"/tmp/error_{username}_{ts}.png")
        except Exception:
            pass
        raise
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def build_dag(app_id: int, app_name: str) -> DAG:
    safe = sanitize(app_name)
    dag_id = f"reload_app_{app_id}_{safe}"

    default_args = {
        "owner": "admin",
        "depends_on_past": False,
        "start_date": datetime(2025, 1, 1),
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    }

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f"Extend PythonAnywhere web app: {app_name}",
        schedule_interval=timedelta(weeks=1),
        catchup=False,
        tags=["pythonanywhere", "reload_app"],
        max_active_runs=1,
    ) as dag:
        replicas = fetch_replicas(app_id)
        extends = extend_replica.partial(app_name=app_name).expand(
            replica=replicas
        )
        done = DummyOperator(task_id="all_extend_done")
        extends >> done
    return dag


# Generate DAGs at parse time
APPS = get_apps_from_server()
logger.info(f"DAG parse: generating {len(APPS)} DAGs")

for _app in APPS:
    _dag = build_dag(_app["id"], _app["name"])
    globals()[_dag.dag_id] = _dag