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
import shutil
import tempfile
import logging
import time
from airflow.models import Variable

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Function to fetch data from API
def fetch_app_data():
    """
    Fetch app data from the API using authentication token from Airflow Variables
    """
    try:
        from airflow.models import Variable
        auth_token = Variable.get("AUTH_TOKEN")
        logger.info("Successfully retrieved AUTH_TOKEN from Airflow Variables")
    except Exception as e:
        logger.error(f"Failed to get AUTH_TOKEN from Variables: {e}")
        # Fallback to environment variable
        auth_token = os.getenv('AUTH_TOKEN')
        if not auth_token:
            raise ValueError("AUTH_TOKEN not found in Airflow Variables or environment variables")
    
    # Try both domains
    domains = [
        "https://sfsdomains1.pythonanywhere.com/apps/19",
        "https://sfsdomains2.pythonanywhere.com/apps/19"
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
            
            # Extract users from replicas
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

# Function to download and set up geckodriver
def download_geckodriver():
    geckodriver_path = '/tmp/geckodriver'
    if os.path.exists(geckodriver_path):
        return geckodriver_path

    # Add GitHub token if available (optional but recommended)
    headers = {}
    if os.environ.get('GITHUB_TOKEN'):
        headers['Authorization'] = f"token {os.environ['GITHUB_TOKEN']}"

    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            url = "https://api.github.com/repos/mozilla/geckodriver/releases/latest"
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            
            latest_release = response.json()
            download_url = next(
                asset['browser_download_url'] for asset in latest_release['assets']
                if 'linux64' in asset['browser_download_url']
            )
            
            # Add retry logic for the download
            for attempt in range(3):
                try:
                    driver_response = requests.get(download_url, timeout=30)
                    driver_response.raise_for_status()
                    break
                except (requests.exceptions.RequestException, ConnectionError) as e:
                    if attempt == 2:  # Last attempt
                        raise
                    time.sleep(5)  # Wait before retrying
                    
            tar_path = os.path.join(temp_dir, 'geckodriver.tar.gz')
            with open(tar_path, 'wb') as f:
                f.write(driver_response.content)

            os.system(f'tar -xzf {tar_path} -C {temp_dir}')
            extracted_path = os.path.join(temp_dir, 'geckodriver')
            shutil.move(extracted_path, geckodriver_path)
            
        except Exception as e:
            logger.error(f"Failed to download geckodriver: {e}")
            raise

    os.chmod(geckodriver_path, 0o755)
    return geckodriver_path

# Function to log in to PythonAnywhere using Firefox
def login_to_pythonanywhere(username, password):
    firefox_options = FirefoxOptions()
    firefox_options.add_argument('--headless')
    firefox_options.add_argument('--no-sandbox')
    firefox_options.add_argument('--disable-dev-shm-usage')
    
    # Specify the path to the geckodriver
    geckodriver_path = download_geckodriver()
    service = FirefoxService(executable_path=geckodriver_path)
    
    driver = webdriver.Firefox(service=service, options=firefox_options)
    driver.set_window_size(1920, 1080)

    try:
        # Navigate to PythonAnywhere login page
        driver.get("https://www.pythonanywhere.com/login/?next=/")
        username_field = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="id_auth-username"]'))
        )
        username_field.send_keys(username)
        password_field = driver.find_element(By.XPATH, '//*[@id="id_auth-password"]')
        password_field.send_keys(password)
        login_button = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//button[@type="submit" and contains(text(), "Log in")]'))
        )
        login_button.click()
        WebDriverWait(driver, 10).until(
            EC.url_contains("https://www.pythonanywhere.com/user/")
        )
        return driver
    except Exception as e:
        if driver.session_id:
            driver.save_screenshot("login_error.png")
        logger.error(f"Error during login: {e}")
        driver.quit()
        raise

# Function to extend web app
def extend_web_app(username, password, **kwargs):
    driver = None
    try:
        driver = login_to_pythonanywhere(username, password)
        driver.get(f"https://www.pythonanywhere.com/user/{username}/webapps/")
        extend_button = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//input[@type="submit" and @value="Run until 3 months from today"]'))
        )
        extend_button.click()
        logger.info(f"Web app extended for user: {username}")
        kwargs['ti'].xcom_push(key='username', value=username)
        kwargs['ti'].xcom_push(key='password', value=password)
    except Exception as e:
        logger.error(f"Error extending web app for user {username}: {e}")
        raise  # Raise the exception to mark the task as failed
    finally:
        if driver:
            driver.quit()

# Define the DAG
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'extend_selfstudy_course_apps',
    default_args=default_args,
    description='Automate PythonAnywhere tasks using API data',
    schedule_interval=timedelta(weeks=1),
    catchup=False,  # Prevent backfilling
)

# Function to create tasks for each user
def create_tasks():
    users = fetch_app_data()
    previous_task = None

    for user in users:
        username = user["username"]
        password = user["password"]

        # Create a task for the user
        task = PythonOperator(
            task_id=f'process_user_{username}',
            python_callable=extend_web_app,
            op_kwargs={'username': username, 'password': password},
            provide_context=True,
            dag=dag,
        )

        # Set the dependency: current task depends on the previous task
        if previous_task:
            previous_task >> task

        # Update the previous task
        previous_task = task

# Create the tasks dynamically based on the users from the API
create_tasks()