import os
import time
import pytest
import requests
import socket
import logging
from urllib.parse import urljoin
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dashboard_host = os.getenv("DASHBOARD_HOST", "emqx")
dashboard_port = os.getenv("DASHBOARD_PORT", "18083")

@pytest.fixture
def driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    # Use hardcoded path if it exists (for Docker), otherwise let Selenium manage it
    chromedriver_path = "/usr/bin/chromedriver"
    if os.path.exists(chromedriver_path):
        service = webdriver.ChromeService(executable_path=chromedriver_path)
        _driver = webdriver.Chrome(options=options, service=service)
    else:
        _driver = webdriver.Chrome(options=options)
    yield _driver
    _driver.quit()

def is_port_open(host, port, timeout=1):
    """Checks if a network port is open and connectable."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        return True
    except (socket.timeout, ConnectionRefusedError):
        return False
    finally:
        s.close()

@pytest.fixture(autouse=True)
def dashboard_url():
    max_wait = 30
    for i in range(max_wait):
        if is_port_open(dashboard_host, int(dashboard_port)):
            return f"http://{dashboard_host}:{dashboard_port}"
        time.sleep(1)
    raise ConnectionError(f"Dashboard at {dashboard_host}:{dashboard_port} is not ready after {max_wait} seconds.")

def login(driver, dashboard_url):
    USERNAME_INPUT = (By.XPATH, "//div[@class='login']//form//input[@type='text']")
    PASSWORD_INPUT = (By.XPATH, "//div[@class='login']//form//input[@type='password']")
    LOGIN_BUTTON = (By.XPATH, "//div[@class='login']//form//button")

    # admin is set in CI jobs, hence as default value
    password = os.getenv("EMQX_DASHBOARD__DEFAULT_PASSWORD", "admin")
    driver.get(dashboard_url)
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, "login"))
    )
    assert "EMQX Dashboard" == driver.title
    assert f"{dashboard_url}/#/login?to=/dashboard/overview" == driver.current_url
    driver.execute_script("window.localStorage.setItem('licenseTipVisible','false');")

    try:
        username_field = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(USERNAME_INPUT)
        )
        username_field.send_keys("admin")
        password_field = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(PASSWORD_INPUT)
        )
        password_field.send_keys(password)
        login_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(LOGIN_BUTTON)
        )
        login_button.click()

    except TimeoutException:
        print("Failed to login after 10+10+10 seconds.")
        raise Exception("Could not interact with login form.")

    dest_url = urljoin(dashboard_url, "/#/dashboard/overview")
    ensure_current_url(driver, dest_url)
    assert len(driver.find_elements(By.XPATH, "//div[@class='login']")) == 0
    logger.info(f"Logged in to {dashboard_url}")

def ensure_current_url(driver, url, timeout=10):
    try:
        WebDriverWait(driver, timeout).until(EC.url_to_be(url))
    except TimeoutException:
        raise Exception(f"Failed to load URL '{url}' within {timeout} seconds. Current URL is '{driver.current_url}'")

def wait_for_title_text(driver, text, timeout=10):
    """Waits for the page title to contain the specific text."""
    # Wait for the app to be loaded and have content
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )
    WebDriverWait(driver, timeout).until(
        lambda d: len(d.find_elements(By.ID, "app")) > 0 and
                 d.find_element(By.ID, "app").text.strip() != ""
    )

    # Wait for the text to appear in the app
    WebDriverWait(driver, timeout).until(
        lambda d: text.lower() in d.find_element(By.ID, "app").text.lower()
    )

def test_basic(driver, dashboard_url):
    login(driver, dashboard_url)
    logger.info(f"Current URL: {driver.current_url}")
    # Wait a bit for React to render
    time.sleep(2)
    wait_for_title_text(driver, "Cluster Overview")

def test_log(driver, dashboard_url):
    login(driver, dashboard_url)
    logger.info(f"Current URL: {driver.current_url}")
    dest_url = urljoin(dashboard_url, "/#/log")
    driver.get(dest_url)
    ensure_current_url(driver, dest_url)
    # Wait a bit for React to render
    time.sleep(2)
    wait_for_title_text(driver, "Logging")

    # Wait for the form to be present
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, "//div[@id='app']//form"))
    )

    labels_to_find = ["Enable Log Handler", "Log Level", "Log Formatter", "Time Offset"]
    for label_text in labels_to_find:
        label = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, f"//div[@id='app']//form//label[contains(., '{label_text}')]"))
        )
        label_for = label.get_attribute("for")
        if label_for:
            assert driver.find_elements(By.ID, label_for), f"Label '{label_text}' found but associated element with id '{label_for}' not found"

def fetch_version_info(dashboard_url):
    status_url = urljoin(dashboard_url, "/status?format=json")
    response = requests.get(status_url)
    response.raise_for_status()
    return response.json()

def parse_version(version_str):
    prefix_major, minor, _ = version_str.split('.', 2)
    prefix = prefix_major[:1]
    major = prefix_major[1:]
    return prefix, major + '.' + minor

def fetch_version(url):
    info = fetch_version_info(url)
    version_str = info['rel_vsn']
    return parse_version(version_str)

def test_docs_link(driver, dashboard_url):
    login(driver, dashboard_url)
    logger.info(f"Current URL: {driver.current_url}")
    xpath_link_help = "//div[@id='app']//div[@class='nav-header']//a[contains(@class, 'link-help')]"
    # retry up to 5 times
    for _ in range(5):
        try:
            link_help = driver.find_element(By.XPATH, xpath_link_help)
            break
        except NoSuchElementException:
            time.sleep(1)
    else:
        raise AssertionError("Cannot find the help link")
    driver.execute_script("arguments[0].click();", link_help)

    prefix, emqx_version = fetch_version(dashboard_url)
    # it's v5.x in the url
    emqx_version = 'v' + emqx_version

    docs_base_url = "https://docs.emqx.com/en/emqx"

    docs_url = f"{docs_base_url}/{emqx_version}"
    xpath = f"//div[@id='app']//div[@class='nav-header']//a[@href[starts-with(.,'{docs_url}')]]"

    try:
        driver.find_element(By.XPATH, xpath)
    except NoSuchElementException:
        raise AssertionError(f"Cannot find the doc URL for version {emqx_version}, please make sure the dashboard package is up to date.")
