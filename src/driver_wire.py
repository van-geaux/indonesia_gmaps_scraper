from webdriver_manager.chrome import ChromeDriverManager

from seleniumwire import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

from src.backend import *
from src.logger import logger
from src.utilities import *

def get_driver_wire(config):
    try:
        logger.debug(f'Setting selenium-wire options')

        proxy_host = config['Proxy'].get('Domain')
        proxy_port = config['Proxy'].get('Port')
        proxy_user = config['Proxy'].get('User')
        proxy_pass = config['Proxy'].get('Password')

        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        # chrome_options.add_argument("--disable-software-rasterizer")
        # chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920x1080")
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_argument("--silent")

        seleniumwire_options = {
            'proxy': {
                'http': f'http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_port}',
                'https': f'http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_port}'
            }
        }
    except Exception as e:
        logger.error(f'Setting selenium-wire options failed: {e}')

    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),
                                options=chrome_options,
                                seleniumwire_options=seleniumwire_options)
    except:
        try:
            driver = webdriver.Chrome(service=Service('driver/chromedriver.exe'), options=chrome_options,
                                    seleniumwire_options=seleniumwire_options)
        except Exception as e:
            logger.error(f'Creating driver failed: {e}')
            raise

    return driver