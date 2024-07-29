from webdriver_manager.chrome import ChromeDriverManager

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from src.backend import *
from src.logger import logger
from src.utilities import *

from base64 import b64encode
import os
import re
import zipfile

def get_driver(config):
    try:
        logger.debug(f'Setting chrome options')

        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920x1080")

        chrome_options.add_argument("--log-level=3")
        chrome_options.add_argument("--silent")

        capabilities = webdriver.DesiredCapabilities.CHROME
    except Exception as e:
        logger.error(f'Setting chrome options failed: {e}')
    
    try:
        logger.debug(f'Setting selenium proxy')
        if config['Proxy'].get('Domain'):
            prox = Proxy()
            prox.proxy_type = ProxyType.MANUAL
            prox.ssl_proxy = f"http://{config['Proxy'].get('User')}:{config['Proxy'].get('Password')}@{config['Proxy'].get('Domain')}:{config['Proxy'].get('Port')}"

            prox.add_to_capabilities(capabilities)
        else:
            logger.info('Not using proxy')
    except Exception as e:
        logger.error(f'Setting selenium proxy failed: {e}')

    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options, desired_capabilities=capabilities)
    except Exception as e:
        try:
            driver = webdriver.Chrome(service=Service('driver/chromedriver.exe'), options=chrome_options, desired_capabilities=capabilities)
        except Exception as e:
            logger.error(f'Creating driver failed: {e}')
        
    return driver

def get_driver_extension(config):
    try:
        logger.debug(f'Setting chrome options')

        if not os.path.exists('backend/proxy_auth_extension.zip'):

            proxy_host = config['Proxy'].get('Domain')
            proxy_port = config['Proxy'].get('Port')
            proxy_user = config['Proxy'].get('User')
            proxy_pass = config['Proxy'].get('Password')

            # Create a manifest file
            manifest_json = """
            {
                "version": "1.0.0",
                "manifest_version": 2,
                "name": "Chrome Proxy",
                "permissions": [
                    "proxy",
                    "tabs",
                    "unlimitedStorage",
                    "storage",
                    "<all_urls>",
                    "webRequest",
                    "webRequestBlocking"
                ],
                "background": {
                    "scripts": ["background.js"]
                },
                "minimum_chrome_version":"22.0.0"
            }
            """

            # Create a background.js file
            background_js = f"""
            var config = {{
                mode: "fixed_servers",
                rules: {{
                    singleProxy: {{
                        scheme: "http",
                        host: "{proxy_host}",
                        port: parseInt({proxy_port})
                    }},
                    bypassList: ["localhost"]
                }}
            }};

            chrome.proxy.settings.set({{value: config, scope: "regular"}}, function() {{}});
            function callbackFn(details) {{
                return {{
                    authCredentials: {{
                        username: "{proxy_user}",
                        password: "{proxy_pass}"
                    }}
                }};
            }}
            chrome.webRequest.onAuthRequired.addListener(
                callbackFn,
                {{urls: ["<all_urls>"]}},
                ['blocking']
            );
            """

            # Create the extension files
            extension_dir = 'backend/proxy_extension'
            os.makedirs(extension_dir, exist_ok=True)
            with open(os.path.join(extension_dir, 'manifest.json'), 'w') as f:
                f.write(manifest_json)
            with open(os.path.join(extension_dir, 'background.js'), 'w') as f:
                f.write(background_js)

            # Create the extension zip file
            with zipfile.ZipFile('backend/proxy_auth_extension.zip', 'w') as zp:
                zp.write(os.path.join(extension_dir, 'manifest.json'), 'manifest.json')
                zp.write(os.path.join(extension_dir, 'background.js'), 'background.js')

        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--window-size=1920x1080")
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_argument("--silent")

        chrome_options.add_extension('backend/proxy_auth_extension.zip')

    except Exception as e:
        logger.error(f'Setting chrome options failed: {e}')

    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options) #desired_capabilities=capabilities
    except:
        try:
            driver = webdriver.Chrome(service=Service('driver/chromedriver.exe'), options=chrome_options) #desired_capabilities=capabilities
        except Exception as e:
            logger.error(f'Creating driver failed: {e}')

    return driver

# def get_driver_devtools(config):
#     try:
#         logger.debug(f'Setting chrome options')

#         proxy_host = config['Proxy'].get('Domain')
#         proxy_port = config['Proxy'].get('Port')
#         proxy_user = config['Proxy'].get('User')
#         proxy_pass = config['Proxy'].get('Password')

#         # Validate proxy configuration
#         if not all([proxy_host, proxy_port, proxy_user, proxy_pass]):
#             raise ValueError("Incomplete proxy configuration")

#     except Exception as e:
#         logger.error(f'Setting chrome options failed: {e}')
#         return None

#     # Encode the proxy credentials
#     proxy_credentials = f'{proxy_user}:{proxy_pass}'
#     encoded_credentials = base64.b64encode(proxy_credentials.encode('utf-8')).decode('utf-8')

#     # Setup Chrome options
#     chrome_options = Options()
#     chrome_options.add_argument("--headless")  # Run in headless mode
#     chrome_options.add_argument("--disable-gpu")
#     chrome_options.add_argument("--no-sandbox")
#     chrome_options.add_argument(f'--proxy-server=http://{proxy_host}:{proxy_port}')
#     chrome_options.add_argument("--window-size=1920x1080")
#     chrome_options.add_argument("--log-level=3")
#     chrome_options.add_argument("--silent")

#     # Initialize the Chrome driver
#     driver = None
#     try:
#         driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
#     except Exception as e:
#         logger.error(f'Failed to initialize ChromeDriver using webdriver_manager: {e}')
#         try:
#             driver = webdriver.Chrome(service=Service('driver/chromedriver.exe'), options=chrome_options)
#         except Exception as e:
#             logger.error(f'Failed to initialize ChromeDriver using local path: {e}')
#             return None

#     if driver is None:
#         logger.error("Failed to initialize ChromeDriver")
#         return None

#     # Configure the DevTools Protocol to handle authentication
#     try:
#         driver.execute_cdp_cmd('Network.enable', {})
#         driver.execute_cdp_cmd('Network.setExtraHTTPHeaders', {
#             'headers': {
#                 'Proxy-Authorization': 'Basic ' + encoded_credentials
#             }
#         })
#     except Exception as e:
#         logger.error(f'Failed to set DevTools Protocol commands: {e}')
#         driver.quit()
#         return None

#     return driver