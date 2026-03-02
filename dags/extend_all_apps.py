from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import logging
import os
import requests
from airflow.models import Variable

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
APP_IDS = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24]
APP_NAMES = {
    8: "SelfStudy Domains",
    9: "SelfStudy Chat",
    10: "All Chat",
    11: "Lab",
    12: "Live Course",
    13: "User Profile",
    14: "SelfStudy OTP",
    15: "SelfStudy Auth",
    16: "Notifications",
    17: "Runbooks",
    18: "SelfStudy Media",
    19: "SelfStudy Course",
    20: "Exam",
    21: "Proctor",
    22: "Subscriptions",
    23: "Payment",
    24: "Certificate"
}
DOMAINS = [
    "https://sfsdomains1.pythonanywhere.com",
    "https://sfsdomains2.pythonanywhere.com"
]

# Force sequential execution: only one task runs at a time
MAX_CONCURRENT_TASKS = 1

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------
# Task: Fetch replicas for a given app
# ------------------------------------------------------------
@task
def fetch_replicas(app_id: int):
    """
    Fetch replicas for a specific app. Returns list of dicts with username, password.
    """
    auth_token = Variable.get("AUTH_TOKEN")
    headers = {
        'Authorization': f'Token {auth_token}',
        'Content-Type': 'application/json'
    }
    for domain in DOMAINS:
        url = f"{domain}/apps/{app_id}"
        try:
            logger.info(f"Fetching from {url}")
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            replicas = [
                {
                    'username': rep['replica_username'],
                    'password': rep['replica_password']
                }
                for rep in data.get('replicas', [])
            ]
            logger.info(f"App {app_id}: {len(replicas)} replicas")
            return replicas
        except Exception as e:
            logger.warning(f"Failed to fetch from {url}: {e}")
            continue
    logger.error(f"Could not fetch data for app {app_id} from any domain")
    return []  # return empty list so no tasks are created

# ------------------------------------------------------------
# Task: Extend a single replica (the actual Selenium work)
# ------------------------------------------------------------
@task
def extend_replica(replica: dict, app_name: str):
    """
    Perform the web app extension for one replica.
    """
    import time
    from selenium import webdriver
    from selenium.webdriver.firefox.service import Service as FirefoxService
    from selenium.webdriver.firefox.options import Options as FirefoxOptions
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException

    username = replica['username']
    password = replica['password']
    logger.info(f"Starting extension for {username} (app: {app_name})")

    def clear_browser_data(driver):
        try:
            driver.delete_all_cookies()
            driver.execute_script("window.localStorage.clear();")
            driver.execute_script("window.sessionStorage.clear();")
            logger.info("Browser data cleared")
        except Exception as e:
            logger.warning(f"Could not clear browser data: {e}")

    firefox_options = FirefoxOptions()
    firefox_options.add_argument('--headless')
    firefox_options.add_argument('--no-sandbox')
    firefox_options.add_argument('--disable-dev-shm-usage')
    firefox_options.set_preference("general.useragent.override",
                                   "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0")

    service = FirefoxService(executable_path="/usr/local/bin/geckodriver")
    driver = webdriver.Firefox(service=service, options=firefox_options)
    driver.set_window_size(1920, 1080)

    try:
        # --- Login ---
        clear_browser_data(driver)
        driver.get("https://www.pythonanywhere.com/login/?next=/")
        logger.info(f"Login page loaded, title: {driver.title}")

        username_field = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, 'id_auth-username'))
        )
        username_field.clear()
        username_field.send_keys(username)

        password_field = driver.find_element(By.ID, 'id_auth-password')
        password_field.clear()
        password_field.send_keys(password)

        login_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, '//button[contains(text(), "Log in")]'))
        )
        login_button.click()

        WebDriverWait(driver, 20).until(
            EC.any_of(
                EC.presence_of_element_located((By.XPATH, f'//a[contains(@href, "/user/{username}/")]')),
                EC.presence_of_element_located((By.XPATH, f'//span[contains(text(), "{username}")]')),
                EC.url_contains("/user/")
            )
        )
        logger.info("Login successful")

        # --- Go to web apps page ---
        driver.get(f"https://www.pythonanywhere.com/user/{username}/webapps/")
        logger.info(f"Web apps page loaded, title: {driver.title}")

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, 'body'))
        )

        if "You haven't created any web apps" in driver.page_source:
            logger.warning(f"User {username} has no web apps. Skipping.")
            return {"status": "no_webapps", "username": username}

        # --- Find and click extend button ---
        extend_selectors = [
            '//input[@type="submit" and contains(@value, "Run until")]',
            '//button[contains(text(), "Run until")]',
            '//input[contains(@value, "Run until")]',
            '//input[@type="submit" and contains(@value, "Extend")]',
            '//button[contains(text(), "Extend")]',
            '//form//input[@type="submit"][contains(@value, "Run")]'
        ]

        extend_button = None
        for selector in extend_selectors:
            try:
                extend_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, selector))
                )
                logger.info(f"Found extend button with selector: {selector}")
                break
            except TimeoutException:
                continue

        if not extend_button:
            page_text = driver.page_source
            if "Your web app will expire on" in page_text or "expires on" in page_text:
                logger.info(f"App not due for extension – no button found.")
                return {"status": "not_due", "username": username}
            else:
                logger.error(f"No extend button found and no expiration message.")
                driver.save_screenshot(f"/tmp/no_button_{username}.png")
                with open(f"/tmp/page_{username}.html", "w") as f:
                    f.write(driver.page_source)
                raise Exception(f"Extend button not found for {username}")

        driver.execute_script("arguments[0].scrollIntoView(true);", extend_button)
        time.sleep(1)
        extend_button.click()
        logger.info("Extend button clicked")

        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.XPATH, '//div[contains(@class, "alert-success")]'))
            )
            logger.info("Extension confirmed (success message)")
        except TimeoutException:
            time.sleep(5)
            logger.info("Extension likely completed (no success message)")

        return {"status": "extended", "username": username}

    except Exception as e:
        logger.error(f"Error processing user {username}: {e}")
        if driver:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            driver.save_screenshot(f"/tmp/error_{username}_{timestamp}.png")
            with open(f"/tmp/page_{username}_{timestamp}.html", "w") as f:
                f.write(driver.page_source)
        raise
    finally:
        if driver:
            driver.quit()

# ------------------------------------------------------------
# DAG Definition
# ------------------------------------------------------------
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='extend_all_apps_professional',
    default_args=default_args,
    description='Extend all PythonAnywhere web apps – sequentially by app and by replica',
    schedule_interval=timedelta(weeks=1),
    catchup=False,
    tags=['pythonanywhere', 'sequential'],
    max_active_tasks=MAX_CONCURRENT_TASKS,
    max_active_runs=1,
) as dag:

    # This will hold the last task of the previous app group
    prev_group_last = None

    for app_id in APP_IDS:
        app_name = APP_NAMES.get(app_id, f"App_{app_id}")
        # Sanitize group_id
        safe_name = app_name.replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '')
        group_id = f"app_{app_id}_{safe_name}"

        @task_group(group_id=group_id)
        def app_group(app_id=app_id, app_name=app_name):
            # Fetch replicas for this app
            replicas = fetch_replicas(app_id)

            # Create mapped extend tasks (they will run sequentially because max_active_tasks=1)
            extend_tasks = extend_replica.partial(app_name=app_name).expand(replica=replicas)

            # Dummy task to mark completion of this app's extensions
            all_done = DummyOperator(task_id=f"all_extend_done")
            all_done.set_upstream(extend_tasks)
            return all_done

        # Instantiate the group
        group_last = app_group()

        # Chain: previous app's last dummy -> this app's group
        if prev_group_last:
            prev_group_last >> group_last
        prev_group_last = group_last