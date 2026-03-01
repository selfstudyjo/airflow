from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import requests
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import logging
import time
from airflow.models import Variable

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_app_data():
    """
    Fetch app data from the API using authentication token from Airflow Variables
    """
    try:
        auth_token = Variable.get("AUTH_TOKEN")
        logger.info("Successfully retrieved AUTH_TOKEN from Airflow Variables")
    except Exception as e:
        logger.error(f"Failed to get AUTH_TOKEN from Variables: {e}")
        auth_token = os.getenv('AUTH_TOKEN')
        if not auth_token:
            raise ValueError("AUTH_TOKEN not found in Airflow Variables or environment variables")

    domains = [
        "https://sfsdomains1.pythonanywhere.com/apps/9",
        "https://sfsdomains2.pythonanywhere.com/apps/9"
    ]

    headers = {
        'Authorization': f'Token {auth_token}',
        'Content-Type': 'application/json'
    }

    for domain_url in domains:
        try:
            logger.info(f"Fetching data from: {domain_url}")
            response = requests.get(domain_url, headers=headers, timeout=30)
            response.raise_for_status()
            app_data = response.json()
            logger.info(f"Successfully fetched data from {domain_url}")

            users = []
            for replica in app_data.get('replicas', []):
                users.append({
                    "username": replica["replica_username"],
                    "password": replica["replica_password"]
                })
            logger.info(f"Found {len(users)} replicas")
            return users
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to fetch from {domain_url}: {e}")
            continue

    raise Exception("Failed to fetch data from all available domains")

def clear_browser_data(driver):
    """Clear cookies and local storage to ensure a fresh session."""
    try:
        driver.delete_all_cookies()
        driver.execute_script("window.localStorage.clear();")
        driver.execute_script("window.sessionStorage.clear();")
        logger.info("Browser data cleared")
    except Exception as e:
        logger.warning(f"Could not clear browser data: {e}")

def login_to_pythonanywhere(username, password):
    """
    Log in to PythonAnywhere using headless Firefox.
    Returns the WebDriver instance if successful.
    """
    firefox_options = FirefoxOptions()
    firefox_options.add_argument('--headless')
    firefox_options.add_argument('--no-sandbox')
    firefox_options.add_argument('--disable-dev-shm-usage')
    # Realistic user agent to avoid detection
    firefox_options.set_preference("general.useragent.override",
                                   "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0")

    service = FirefoxService(executable_path="/usr/local/bin/geckodriver")
    driver = webdriver.Firefox(service=service, options=firefox_options)
    driver.set_window_size(1920, 1080)

    try:
        clear_browser_data(driver)
        driver.get("https://www.pythonanywhere.com/login/?next=/")
        logger.info(f"Login page loaded for {username}, title: {driver.title}")

        # Wait for and fill username
        username_field = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, 'id_auth-username'))
        )
        username_field.clear()
        username_field.send_keys(username)

        # Fill password
        password_field = driver.find_element(By.ID, 'id_auth-password')
        password_field.clear()
        password_field.send_keys(password)

        # Click login button
        login_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, '//button[contains(text(), "Log in")]'))
        )
        login_button.click()

        # Wait for login to complete (check for user-specific elements)
        WebDriverWait(driver, 20).until(
            EC.any_of(
                EC.presence_of_element_located((By.XPATH, f'//a[contains(@href, "/user/{username}/")]')),
                EC.presence_of_element_located((By.XPATH, f'//span[contains(text(), "{username}")]')),
                EC.url_contains("/user/")
            )
        )
        logger.info(f"Login successful for {username}")
        return driver
    except Exception as e:
        logger.error(f"Login failed for {username}: {e}")
        if driver:
            driver.save_screenshot(f"/tmp/login_error_{username}.png")
        driver.quit()
        raise

def extend_web_app(username, password, **kwargs):
    """
    Navigate to the user's web apps page and attempt to extend the app.
    Handles cases where the extend button is not present.
    """
    driver = None
    try:
        driver = login_to_pythonanywhere(username, password)
        driver.get(f"https://www.pythonanywhere.com/user/{username}/webapps/")
        logger.info(f"Web apps page loaded for {username}, title: {driver.title}")

        # Wait for body to ensure page is fully loaded
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, 'body'))
        )

        # Check if user has no web apps
        if "You haven't created any web apps" in driver.page_source:
            logger.warning(f"User {username} has no web apps to extend. Skipping.")
            kwargs['ti'].xcom_push(key='status', value='no_webapps')
            return

        # Define multiple selectors for the extend button
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
            # Check if there's a message indicating when the app expires
            page_text = driver.page_source
            if "Your web app will expire on" in page_text or "expires on" in page_text:
                logger.info(f"App for {username} is not due for extension yet. No button found.")
                kwargs['ti'].xcom_push(key='status', value='not_due')
                return
            else:
                # Unexpected state – save screenshot and fail
                logger.error(f"No extend button found for {username} and no expiration message.")
                driver.save_screenshot(f"/tmp/no_button_{username}.png")
                with open(f"/tmp/page_{username}.html", "w") as f:
                    f.write(driver.page_source)
                raise Exception(f"Extend button not found for {username}")

        # Click the button
        driver.execute_script("arguments[0].scrollIntoView(true);", extend_button)
        time.sleep(1)
        extend_button.click()
        logger.info(f"Extend button clicked for {username}")

        # Wait for success message or page reload
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.XPATH, '//div[contains(@class, "alert-success")]'))
            )
            logger.info(f"Extension confirmed for {username}")
        except TimeoutException:
            # Sometimes no success message, just wait a bit
            time.sleep(5)
            logger.info(f"Extension likely completed for {username} (no success message)")

        kwargs['ti'].xcom_push(key='username', value=username)
        kwargs['ti'].xcom_push(key='status', value='extended')

    except Exception as e:
        logger.error(f"Error extending web app for user {username}: {e}")
        if driver:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            driver.save_screenshot(f"/tmp/error_{username}_{timestamp}.png")
            with open(f"/tmp/page_{username}_{timestamp}.html", "w") as f:
                f.write(driver.page_source)
        raise
    finally:
        if driver:
            driver.quit()

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'extend_chat_apps',
    default_args=default_args,
    description='Automate PythonAnywhere web app extension using API data',
    schedule_interval=timedelta(weeks=1),
    catchup=False,
)

def create_tasks():
    users = fetch_app_data()
    previous_task = None

    for user in users:
        username = user["username"]
        password = user["password"]

        task = PythonOperator(
            task_id=f'process_user_{username}',
            python_callable=extend_web_app,
            op_kwargs={'username': username, 'password': password},
            provide_context=True,
            dag=dag,
        )

        if previous_task:
            previous_task >> task
        previous_task = task

create_tasks()