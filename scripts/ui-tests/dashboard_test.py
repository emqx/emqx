import os
import time
import unittest
import pytest
import requests
from urllib.parse import urljoin
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common import utils
from selenium.common.exceptions import NoSuchElementException

@pytest.fixture
def driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    _driver = webdriver.Chrome(options=options)
    yield _driver
    _driver.quit()

@pytest.fixture(autouse=True)
def dashboard_url(dashboard_host, dashboard_port):
    count = 0
    while utils.is_connectable(port=dashboard_port, host=dashboard_host) is False:
        if count == 30:
            raise Exception("Dashboard is not ready")
        count += 1
        time.sleep(1)
    return f"http://{dashboard_host}:{dashboard_port}"

@pytest.fixture
def login(driver, dashboard_url):
    # admin is set in CI jobs, hence as default value
    password = os.getenv("EMQX_DASHBOARD__DEFAULT_PASSWORD", "admin")
    driver.get(dashboard_url)
    assert "EMQX Dashboard" == driver.title
    assert f"{dashboard_url}/#/login?to=/dashboard/overview" == driver.current_url
    driver.find_element(By.XPATH, "//div[@class='login']//form[1]//input[@type='text']").send_keys("admin")
    driver.find_element(By.XPATH, "//div[@class='login']//form[1]//input[@type='password']").send_keys(password)
    driver.find_element(By.XPATH, "//div[@class='login']//form[1]//button[1]").click()
    dest_url = urljoin(dashboard_url, "/#/dashboard/overview")
    driver.get(dest_url)
    ensure_current_url(driver, dest_url)

def ensure_current_url(driver, url):
    count = 0
    while url != driver.current_url:
        if count == 10:
            raise Exception(f"Failed to load {url}")
        count += 1
        time.sleep(1)

def title(driver):
    return driver.find_element("xpath", "//div[@id='app']//h1[@class='header-title']")

def wait_title_text(driver, text):
    return WebDriverWait(driver, 10).until(lambda x: title(x).text == text)

def test_basic(driver, login, dashboard_url):
    driver.get(dashboard_url)
    wait_title_text(driver, "Cluster Overview")

def test_log(driver, login, dashboard_url):
    dest_url = urljoin(dashboard_url, "/#/log")
    driver.get(dest_url)
    ensure_current_url(driver, dest_url)
    wait_title_text(driver, "Logging")

    label = driver.find_element(By.XPATH, "//div[@id='app']//form//label[contains(., 'Enable Log Handler')]")
    assert driver.find_elements(By.ID, label.get_attribute("for"))
    label = driver.find_element(By.XPATH, "//div[@id='app']//form//label[contains(., 'Log Level')]")
    assert driver.find_elements(By.ID, label.get_attribute("for"))
    label = driver.find_element(By.XPATH, "//div[@id='app']//form//label[contains(., 'Log Formatter')]")
    assert driver.find_elements(By.ID, label.get_attribute("for"))
    label = driver.find_element(By.XPATH, "//div[@id='app']//form//label[contains(., 'Time Offset')]")
    assert driver.find_elements(By.ID, label.get_attribute("for"))

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

def test_docs_link(driver, login, dashboard_url):
    dest_url = urljoin(dashboard_url, "/#/dashboard/overview")
    driver.get(dest_url)
    ensure_current_url(driver, dest_url)
    xpath_link_help = "//div[@id='app']//div[@class='nav-header']//a[contains(@class, 'link-help')]"
    link_help = driver.find_element(By.XPATH, xpath_link_help)
    driver.execute_script("arguments[0].click();", link_help)

    prefix, emqx_version = fetch_version(dashboard_url)
    # it's v5.x in the url
    emqx_version = 'v' + emqx_version

    if prefix == 'e':
        docs_base_url = "https://docs.emqx.com/en/enterprise"
    else:
        docs_base_url = "https://www.emqx.io/docs/en"

    docs_url = f"{docs_base_url}/{emqx_version}"
    xpath = f"//div[@id='app']//div[@class='nav-header']//a[@href[starts-with(.,'{docs_url}')]]"

    try:
        driver.find_element(By.XPATH, xpath)
    except NoSuchElementException:
        raise AssertionError(f"Cannot find the doc URL for version {emqx_version}, please make sure the dashboard package is up to date.")
