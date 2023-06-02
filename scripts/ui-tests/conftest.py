import pytest
from selenium import webdriver

def pytest_addoption(parser):
    parser.addoption("--dashboard-host", action="store", default="localhost", help="Dashboard host")
    parser.addoption("--dashboard-port", action="store", default="18083", help="Dashboard port")

@pytest.fixture
def dashboard_host(request):
    return request.config.getoption("--dashboard-host")

@pytest.fixture
def dashboard_port(request):
    return request.config.getoption("--dashboard-port")

