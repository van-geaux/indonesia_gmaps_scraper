from alive_progress import alive_bar, alive_it
from bs4 import BeautifulSoup
from datetime import datetime

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
from src.logger import *
from src.utilities import *

import itertools
import json
import pymysql
import re
import requests
import sqlite3
import sys
import threading
import time

# disable SSL warnings
import warnings
from urllib3.exceptions import InsecureRequestWarning
warnings.simplefilter('ignore', InsecureRequestWarning)

# logging.basicConfig(filename='error.log', level=logging.ERROR)

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
        logger.erdebugor(f'Setting selenium proxy')
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
        logger.error(f'Creating driver failed: {e}')
        driver = webdriver.Chrome(service=Service('driver/124.0.6367.207/chromedriver-win32/chromedriver.exe'), options=chrome_options, desired_capabilities=capabilities)

    return driver

def deep_scraper(config):
    try:
        logger.debug(f'Getting configuration')
        database_type = ('sqlite' if config['Data_source']['Local'].get('Location') else config['Data_source']['External'].get('Type').lower())
        category = config.get('Category')
        address_filter = config['Address_level']

        proxy_detail = {
            "https":f"http://{config['Proxy'].get('User')}:{config['Proxy'].get('Password')}@{config['Proxy'].get('Domain')}:{config['Proxy'].get('Port')}"
        }
    except Exception as e:
        logger.error(f'Getting configuration failed: {e}')

    print('')
    logger.info('Checking database, creating if not exists...')
    db_insert(database_type, clean_table_name(category, address_filter), config)

    proxy_count = 0
    proxy_check = ''

    while proxy_count < 10:
        try:
            logger.debug(f'Checking loop config')
            if config['Scrape_address'] == 'loop':
                df_search = create_new_df_search(config, database_type, category, address_filter)
                logger.info(f'Total queries expected in this scraping cycle: {len(df_search)}')
        except Exception as e:
            logger.error(f'Checking loop config failed: {e}')
            raise

        if len(df_search) < 1:
            logger.warning('There are no matching place in the database with what you\'ve configured')
            break
        
        logger.info('Getting new selenium driver...')
        driver = get_driver(config)

        try:
            with alive_bar(calibrate=15, enrich_print=False) as bar:
                for i in range(len(df_search)):
                # for i, r in df_search.iterrows():
                    try:
                        logger.debug(f'Setting value from database')
                        start_time = time.time()
                        dbtime= datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        province = df_search.iloc[i].iloc[0]
                        city = df_search.iloc[i].iloc[1]
                        district = df_search.iloc[i].iloc[2]
                        ward = df_search.iloc[i].iloc[3]
                        search_id = int(df_search.iloc[i].iloc[5])
                        query = f'{category} in {ward}, {district}, {city}, {province}'
                        search_url = create_search_link(query, None, '', 18)
                    except Exception as e:
                        logger.error(f'Setting value from database failed: {e}')
                    
                    try:
                        logger.debug(f'Checking proxy availability')
                        driver.get(search_url)
                        try:
                            WebDriverWait(driver, 10).until(EC.title_contains("Google Maps"))
                            proxy_check = ''
                        except Exception:
                            logger.warning('Proxy failed, getting new selenium driver with new proxy...')
                            proxy_check = 'Proxy failed'
                            break
                    except Exception as e:
                        logger.error(f'Checking proxy availability failed: {e}')
                    
                    try:
                        divSideBar=driver.find_element(By.CSS_SELECTOR, "div[role='feed']")
                    except Exception:
                        logger.info(f'[EMPTY] Query {i+1}/{len(df_search)} 0 data in {ward}, {district}, {city}, {province}')
                        logger.info(f'Total time {time.time() - start_time}')
                        print('')
                        bar()
                        continue
                    
                    keepScrolling=True
                    try:
                        while keepScrolling:
                            try:
                                logger.debug(f'Page scroll')
                                divSideBar.send_keys(Keys.PAGE_DOWN)
                                div_html = driver.find_element(By.TAG_NAME, "html").get_attribute('outerHTML')

                                if "You've reached the end of the list." in div_html or 'Anda telah mencapai akhir daftar.' in div_html:
                                    keepScrolling=False
                            except Exception as e:
                                logger.error(f'Page scroll failed: {e}')
                    except:
                        pass
                    
                    try:
                        logger.debug(f'Getting page html')
                        search_soup = BeautifulSoup(driver.page_source, 'html.parser')
                        targets = search_soup.find("div", {'role': 'feed'}).find_all('div', {'class': False})[:-1]
                        targets_no_ad = [div for div in targets if div.find('div', {'jsaction':True})]
                    except Exception as e:
                        logger.error(f'Getting page html failed: {e}')

                    if targets_no_ad:
                        values = []
                        a = 0
                        try:
                            while True:
                                try:
                                    logger.debug(f'Query {i+1}/{len(df_search)} in {ward}, {district}, {city}, {province}')

                                    name = targets_no_ad[a].find_all("div", {'class':True})[0].find('a')['aria-label']

                                    try:
                                        rating = float(targets_no_ad[a].find_all('span')[4].find_all('span')[0].text.strip().replace(',','.'))
                                    except:
                                        rating = 0

                                    try:
                                        rating_count = int(targets_no_ad[a].find_all("div")[17].find_all("span")[4].text.strip()[1:-1].replace(',',''))
                                    except:
                                        rating_count = 0

                                    google_url = targets_no_ad[a].find_all('a')[0]['href']

                                    try:
                                        logger.debug(f'Getting deep values')
                                        response_deep = requests.get(google_url, proxies=proxy_detail, verify=False)
                                        search_data_deep = response_deep.text
                                        search_soup_deep = BeautifulSoup(search_data_deep, 'html.parser')
                                        scripts_deep = search_soup_deep.find_all('script')

                                        for script_deep in scripts_deep:
                                            if 'window.APP_INITIALIZATION_STATE' in str(script_deep):
                                                data_deep = str(script_deep).split('=',3)[3]
                                                data2_deep = data_deep.rsplit(';',10)[0].split(";window.APP_")[1].split("INITIALIZATION_STATE=")[1]
                                                json_data_deep = json.loads(data2_deep)
                                                type_deep = json_data_deep[3][-1][5:]
                                                json_result_deep = json.loads(type_deep)
                                                break
                                    except:
                                        pass
                                    
                                    try:
                                        longitude = json_result_deep[6][9][2]
                                    except:
                                        try:
                                            coordinate = re.search(r'!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)', targets_no_ad[a].find_all("div")[0].find("a")['href'])
                                            longitude = {coordinate.group(1)}
                                        except:
                                            longitude = ''

                                    try:
                                        latitude = json_result_deep[6][9][3]
                                    except:
                                        try:
                                            latitude = {coordinate.group(2)}
                                        except:
                                            latitude = ''
                                    
                                    try:
                                        address = json_result_deep[6][18]
                                    except:
                                        try:
                                            address = [span for span in targets_no_ad[a].find_all('span', {'aria-hidden':'', 'aria-label':'', 'class':''}) if not span.find('span')][1].text.strip()
                                        except:
                                            address = ''

                                    try:
                                        google_tag = str(json_result_deep[6][13]).strip('[').strip(']').replace('\'','')
                                    except:
                                        try:
                                            google_tag = [span for span in targets_no_ad[a].find_all('span', {'aria-label':'', 'aria-hidden':'', 'class':''}) if not span.find('span')][0].text.strip()
                                        except:
                                            google_tag = ''

                                    values.append((name, longitude, latitude, address, rating, rating_count, google_tag, google_url, ward, district, city, province, category, search_id, dbtime))
                                    a += 1
                                except IndexError:
                                    break
                                except Exception as e:
                                    logger.error(f'[ERROR] Query {i+1}/{len(df_search)} in {ward}, {district}, {city}, {province} failed: {e}')
                                    break
                        
                        finally:
                            if values:
                                if database_type.lower() == 'sqlite':
                                    try:
                                        query = f'INSERT INTO {clean_table_name(category, address_filter)} (NAME, LONGITUDE, LATITUDE, ADDRESS, RATING, RATING_COUNT, GOOGLE_TAGS, GOOGLE_URL, WARD, DISTRICT, CITY, PROVINCE, TYPE, SEARCH_ID, DATA_UPDATE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
                                        with sqlite3.connect(config['Data_source']['Local'].get('Location')) as connection:
                                            cursor = connection.cursor()
                                            cursor.executemany(query, values)
                                    except Exception as e:
                                        logger.error(f'Writing to sqlite failed: {e}')
                    
                                elif database_type.lower() == 'mariadb':
                                    try:
                                        query = f'INSERT INTO {clean_table_name(category, address_filter)} (NAME, LONGITUDE, LATITUDE, ADDRESS, RATING, RATING_COUNT, GOOGLE_TAGS, GOOGLE_URL, WARD, DISTRICT, CITY, PROVINCE, TYPE, SEARCH_ID, DATA_UPDATE) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                                        host, port, user, password, database = [i.replace(' ','') for i in open('authentication/mariadb', 'r').read().split(',')]
                                        connection = pymysql.connect(host=host, port=int(port), user=user, password=password, database=database, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
                        
                                        try:
                                            cursor = connection.cursor()
                                            cursor.executemany(query, values)
                                            connection.commit()
                                        except Exception as e:
                                            logger.error(e)
                                        finally:
                                            connection.close()
                                    except Exception as e:
                                        logger.error(f'Writing to mariadb failed: {e}')
                    
                                else:
                                    logger.error('[ERROR] Database type not recognized, please check if the config.yml is correct.')
                                    # bar()

                                logger.info(f'[SUCCESS] Query {i+1}/{len(df_search)} {a} data in {ward}, {district}, {city}, {province}')
                                # bar()

                            else:
                                logger.info(f'[EMPTY] Query {i+1}/{len(df_search)} 0 data in {ward}, {district}, {city}, {province}')
                                # bar()

                    else:
                        logger.info(f'[EMPTY] Query {i+1}/{len(df_search)} 0 data in {ward}, {district}, {city}, {province}')
                        # bar()

                    logger.info(f'Total time {time.time() - start_time}')
                    print('')
                    bar()

        finally:
            # stop_loading = True
            # loading_thread.join()
            proxy_count += 1
            try:
                print('')
                logger.info('Closing selenium driver...')
                driver.close()
            except Exception:
                pass

        if i+1 == len(df_search):
            break

    if proxy_count > 10 and proxy_check == 'Proxy failed':    
        logger.warning(f'Proxies failed for {proxy_count} times, exiting script.')
        
    logger.info(f'"{category}" scrape finished')