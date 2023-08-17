import time
import unittest
import pytest
from urllib.parse import urljoin
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common import utils

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
    driver.get(dashboard_url)
    assert "EMQX Dashboard" == driver.title
    assert f"{dashboard_url}/#/login?to=/dashboard/overview" == driver.current_url
    driver.find_element(By.XPATH, "//div[@class='login']//form[1]//input[@type='text']").send_keys("admin")
    driver.find_element(By.XPATH, "//div[@class='login']//form[1]//input[@type='password']").send_keys("admin")
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

def wait_title(driver):
    return WebDriverWait(driver, 10).until(lambda x: x.find_element("xpath", "//div[@id='app']//h1[@class='header-title']")) 

def test_basic(driver, login, dashboard_url):
    driver.get(dashboard_url)
    title = wait_title(driver)
    assert "Cluster Overview" == title.text

def test_log(driver, login, dashboard_url):
    dest_url = urljoin(dashboard_url, "/#/log")
    driver.get(dest_url)
    ensure_current_url(driver, dest_url)
    title = wait_title(driver)
    assert "Logging" == title.text

    label = driver.find_element(By.XPATH, "//div[@id='app']//form//label[contains(., 'Enable Log Handler')]")
    assert driver.find_elements(By.ID, label.get_attribute("for"))
    label = driver.find_element(By.XPATH, "//div[@id='app']//form//label[contains(., 'Log Level')]")
    assert driver.find_elements(By.ID, label.get_attribute("for"))
    label = driver.find_element(By.XPATH, "//div[@id='app']//form//label[contains(., 'Log Formatter')]")
    assert driver.find_elements(By.ID, label.get_attribute("for"))
    label = driver.find_element(By.XPATH, "//div[@id='app']//form//label[contains(., 'Time Offset')]")
    assert driver.find_elements(By.ID, label.get_attribute("for"))

