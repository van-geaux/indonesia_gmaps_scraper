from alive_progress import alive_bar, alive_it
from bs4 import BeautifulSoup
from datetime import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from src.backend import db_insert
from src.driver import get_driver, get_driver_extension
from src.logger import logger
from src.utilities import clean_table_name, create_new_df_search, create_search_link

import aiohttp
import aiomysql
import aiosqlite
import asyncio
import json
import pymysql
import re
import requests
import sqlite3
import time

from aiohttp import ClientTimeout, ClientError, ClientConnectorError, ClientTimeout
from aiohttp_socks import ProxyConnector

from stem import Signal
from stem.control import Controller
import time

# disable SSL warnings
import warnings
from urllib3.exceptions import InsecureRequestWarning
warnings.simplefilter('ignore', InsecureRequestWarning)

# async def renew_tor_ip(password='Triplezeta17'):
#     try:
#         with Controller.from_port(port=9051) as controller:
#             controller.authenticate(password=password)
#             controller.signal(Signal.NEWNYM)
#         await asyncio.sleep(5)
#     except Exception as e:
#         logger.error(f"Error renewing Tor IP: {e}")
#         raise

def renew_tor_ip():
    with Controller.from_port(port=9051) as controller:
        controller.authenticate(password='Triplezeta17')
        controller.signal(Signal.NEWNYM)
        # time.sleep(1)

async def fetch(session, google_url, proxy_detail, retries=3):
    timeout = ClientTimeout(total=10)
    for attempt in range(retries):
        try:
            async with session.get(google_url, proxy=proxy_detail, ssl=False, timeout=timeout) as response:
                if attempt > 3:
                    logger.warning('All attempts failed')
                    return None

                response.raise_for_status()

                if attempt > 0:
                    logger.info('Retry attempt succeeded')

                return await response.text()
            
        except (ClientError, ClientConnectorError) as e:
            logger.info(f"Request failed for {google_url}: {e}")
            logger.info(f'Retry attempt {attempt + 1}...')
            await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            logger.info(f'Retry attempt {attempt + 1}...')
            await asyncio.sleep(1)
    return None

async def process_target(session, target, proxy_detail, ward, district, city, province, category, search_id, dbtime):
    try:
        name = target.find_all("div", {'class':True})[0].find('a')['aria-label']

        try:
            rating = float(target.find_all('span')[4].find_all('span')[0].text.strip().replace(',', '.'))
        except:
            rating = 0

        try:
            rating_count = int(target.find_all("div")[17].find_all("span")[4].text.strip()[1:-1].replace(',', ''))
        except:
            rating_count = 0

        google_url = target.find_all('a')[0]['href']

        try:
            logger.info(f'Getting location data for "{name}"')
            try:
                search_data_deep = await fetch(session, google_url, proxy_detail)
            except Exception as e:
                logger.error(f'Can\'t connect to Tor: {e}')
                search_data_deep = await fetch(session, google_url, proxy_detail)

            if not search_data_deep:
                raise ValueError("No data received")

            search_soup_deep = BeautifulSoup(search_data_deep, 'html.parser')
            scripts_deep = search_soup_deep.find_all('script')

            for script_deep in scripts_deep:
                if 'window.APP_INITIALIZATION_STATE' in str(script_deep):
                    data_deep = str(script_deep).split('=', 3)[3]
                    data2_deep = data_deep.rsplit(';', 10)[0].split(";window.APP_")[1].split("INITIALIZATION_STATE=")[1]
                    json_data_deep = json.loads(data2_deep)
                    type_deep = json_data_deep[3][-1][5:]
                    json_result_deep = json.loads(type_deep)
                    break
        except Exception as e:
            json_result_deep = ''
            logger.error(f'Getting location data for "{name}" failed: {e}')
        
        try:
            longitude = json_result_deep[6][9][2]
        except:
            try:
                coordinate = re.search(r'!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)', target.find_all("div")[0].find("a")['href'])
                longitude = float(coordinate.group(1))
            except:
                longitude = ''

        try:
            latitude = json_result_deep[6][9][3]
        except:
            try:
                latitude = float(coordinate.group(2))
            except:
                latitude = ''
        
        try:
            address = json_result_deep[6][18]
        except:
            try:
                address = [span for span in target.find_all('span', {'aria-hidden':'', 'aria-label':'', 'class':''}) if not span.find('span')][1].text.strip()
            except:
                address = ''

        try:
            google_tag = str(json_result_deep[6][13]).strip('[').strip(']').replace('\'','')
        except:
            try:
                google_tag = [span for span in target.find_all('span', {'aria-label':'', 'aria-hidden':'', 'class':''}) if not span.find('span')][0].text.strip()
            except:
                google_tag = ''

        return (name, longitude, latitude, address, rating, rating_count, google_tag, google_url, ward, district, city, province, category, search_id, dbtime)
    except IndexError:
        return None
    except Exception as e:
        logger.error(e)
        raise

async def main(targets_no_ad, proxy_detail, ward, district, city, province, category, search_id, dbtime):
    try:
        async with aiohttp.ClientSession() as session:
            tasks = []
            for i in range(0, len(targets_no_ad)):
                target = targets_no_ad[i]
                task = process_target(session, target, proxy_detail, ward, district, city, province, category, search_id, dbtime)
                tasks.append(task)
            results = await asyncio.gather(*tasks)
        return [result for result in results if result is not None]
    except Exception as e:
        logger.error(e)
        raise

async def write_to_mariadb(host, port, user, password, database, table_name, results):
    conn = await aiomysql.connect(host=host, port=port,
                                  user=user, password=password,
                                  db=database, loop=asyncio.get_event_loop())
    async with conn.cursor() as cur:
        await cur.executemany(f"""
            INSERT INTO {table_name} (NAME, LONGITUDE, LATITUDE, ADDRESS, RATING, RATING_COUNT, GOOGLE_TAGS, GOOGLE_URL, WARD, DISTRICT, CITY, PROVINCE, TYPE, SEARCH_ID, DATA_UPDATE)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, results)
        await conn.commit()
    conn.close()

async def write_to_sqlite(db_location, table_name, results):
    try:
        async with aiosqlite.connect(db_location) as db:
            await db.executemany(f"""
                INSERT INTO {table_name} (NAME, LONGITUDE, LATITUDE, ADDRESS, RATING, RATING_COUNT, GOOGLE_TAGS, GOOGLE_URL, WARD, DISTRICT, CITY, PROVINCE, TYPE, SEARCH_ID, DATA_UPDATE)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, results)
            await db.commit()
    except Exception as e:
        logger.error(e)
        raise

def deep_scraper(config):
    try:
        logger.debug(f'Getting configuration')
        database_type = ('sqlite' if config['Data_source']['Local'].get('Location') else config['Data_source']['External'].get('Type').lower())
        category = config.get('Category')
        address_filter = config['Address_level']

        # proxy_detail = {
        #     "http":f"http://{config['Proxy'].get('User')}:{config['Proxy'].get('Password')}@{config['Proxy'].get('Domain')}:{config['Proxy'].get('Port')}",
        #     "https":f"http://{config['Proxy'].get('User')}:{config['Proxy'].get('Password')}@{config['Proxy'].get('Domain')}:{config['Proxy'].get('Port')}"
        # }
        proxy_detail = f"http://{config['Proxy'].get('User')}:{config['Proxy'].get('Password')}@{config['Proxy'].get('Domain')}:{config['Proxy'].get('Port')}"
        logger.debug(proxy_detail)

    except Exception as e:
        logger.error(f'Getting configuration failed: {e}')

    print('')
    logger.info('Checking database, creating if not exists...')
    table_name = clean_table_name(category, address_filter)
    db_insert(database_type, table_name, config)

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

        if config.get('Query_search_proxy') == True or config.get('Query_search_proxy') is None:
            driver = get_driver_extension(config)
        elif config.get('Query_search_proxy') == False:
            driver = get_driver(config)
        else:
            logger.error('Unrecognized browser configuration option')
            raise

        try:
            with alive_bar(calibrate=15, enrich_print=False) as bar:
                for i in range(len(df_search)):
                # for i, r in df_search.iterrows():
                    try:
                        logger.debug(f'Setting search query from database')
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
                        logger.error(f'Setting search query from database failed: {e}')
                        raise
                    
                    try:
                        logger.debug(f'Checking proxy availability')
                        try:
                            loglevel = config.get('Log_level')
                            if loglevel.lower() == 'debug':
                                driver.get("http://httpbin.org/ip")
                                ip_element = driver.find_element(By.TAG_NAME, "body")
                                current_ip = ip_element.text
                                logger.debug(f"Current IP: {current_ip}")
                        except:
                            pass
                        logger.debug(search_url)
                        driver.get(search_url)
                        WebDriverWait(driver, 10).until(EC.title_contains("Google Maps"))
                        proxy_check = ''
                    except Exception as e:
                        logger.warning(f'Proxy failed, getting new selenium driver with new proxy: {e}')
                        proxy_check = 'Proxy failed'
                        break
                    
                    try:
                        divSideBar=driver.find_element(By.CSS_SELECTOR, "div[role='feed']")
                    except Exception as e:
                        logger.info(f'[EMPTY] Query {i+1}/{len(df_search)} 0 data in {ward}, {district}, {city}, {province}')
                        logger.info(f'Total time {time.time() - start_time}')
                        print('')
                        bar()
                        continue
                    
                    keepScrolling=True
                    try:
                        logger.info(f'Query {i+1}/{len(df_search)} Getting data for {ward}, {district}, {city}, {province}')
                        scroll_count = 0
                        while keepScrolling:
                            try:
                                logger.debug(f'Page scroll')
                                divSideBar.send_keys(Keys.PAGE_DOWN)
                                div_html = driver.find_element(By.TAG_NAME, "html").get_attribute('outerHTML')
                                scroll_count += 1

                                if "You've reached the end of the list." in div_html or 'Anda telah mencapai akhir daftar.' in div_html:
                                    keepScrolling=False
                                    logger.info(f'Total scrolls: {scroll_count}')
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
                        # values = []
                        # a = 0
                        # try:
                        #     logger.info(f'Query {i+1}/{len(df_search)} Getting data for {ward}, {district}, {city}, {province}')
                        #     while True:
                        #         try:
                        #             name = targets_no_ad[a].find_all("div", {'class':True})[0].find('a')['aria-label']

                        #             try:
                        #                 rating = float(targets_no_ad[a].find_all('span')[4].find_all('span')[0].text.strip().replace(',','.'))
                        #             except:
                        #                 rating = 0

                        #             try:
                        #                 rating_count = int(targets_no_ad[a].find_all("div")[17].find_all("span")[4].text.strip()[1:-1].replace(',',''))
                        #             except:
                        #                 rating_count = 0

                        #             google_url = targets_no_ad[a].find_all('a')[0]['href']

                        #             try:
                        #                 logger.info(f'Getting location data for "{name}"')
                        #                 # response_deep = requests.get(google_url, proxies=proxy_detail, verify=False)
                                        
                        #                 renew_tor_ip()
                        #                 response_deep = requests.get(google_url, proxies={
                        #                     'http': 'socks5://localhost:9150',
                        #                     'https': 'socks5://localhost:9150'
                        #                 }, verify=False)

                        #                 search_data_deep = response_deep.text
                        #                 search_soup_deep = BeautifulSoup(search_data_deep, 'html.parser')
                        #                 scripts_deep = search_soup_deep.find_all('script')

                        #                 for script_deep in scripts_deep:
                        #                     if 'window.APP_INITIALIZATION_STATE' in str(script_deep):
                        #                         data_deep = str(script_deep).split('=',3)[3]
                        #                         data2_deep = data_deep.rsplit(';',10)[0].split(";window.APP_")[1].split("INITIALIZATION_STATE=")[1]
                        #                         json_data_deep = json.loads(data2_deep)
                        #                         type_deep = json_data_deep[3][-1][5:]
                        #                         json_result_deep = json.loads(type_deep)
                        #                         break
                        #             except Exception as e:
                        #                 json_result_deep = ''
                        #                 logger.error(f'Getting location data for "{name}" failed: {e}')
                                    
                        #             try:
                        #                 longitude = json_result_deep[6][9][2]
                        #             except:
                        #                 try:
                        #                     coordinate = re.search(r'!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)', targets_no_ad[a].find_all("div")[0].find("a")['href'])
                        #                     longitude = float(coordinate.group(1))
                        #                 except:
                        #                     longitude = ''

                        #             try:
                        #                 latitude = json_result_deep[6][9][3]
                        #             except:
                        #                 try:
                        #                     latitude = float(coordinate.group(2))
                        #                 except:
                        #                     latitude = ''
                                    
                        #             try:
                        #                 address = json_result_deep[6][18]
                        #             except:
                        #                 try:
                        #                     address = [span for span in targets_no_ad[a].find_all('span', {'aria-hidden':'', 'aria-label':'', 'class':''}) if not span.find('span')][1].text.strip()
                        #                 except:
                        #                     address = ''

                        #             try:
                        #                 google_tag = str(json_result_deep[6][13]).strip('[').strip(']').replace('\'','')
                        #             except:
                        #                 try:
                        #                     google_tag = [span for span in targets_no_ad[a].find_all('span', {'aria-label':'', 'aria-hidden':'', 'class':''}) if not span.find('span')][0].text.strip()
                        #                 except:
                        #                     google_tag = ''

                        #             values.append((name, longitude, latitude, address, rating, rating_count, google_tag, google_url, ward, district, city, province, category, search_id, dbtime))
                        #             json_result_deep = ''
                        #             a += 1
                        #         except IndexError:
                        #             break
                        # except Exception as e:
                        #     logger.error(f'[ERROR] Query {i+1}/{len(df_search)} in {ward}, {district}, {city}, {province} failed: {e}')
                        #     break
                        
                        # try:
                        #     if values:
                        #         if database_type.lower() == 'sqlite':
                        #             try:
                        #                 query = f'INSERT INTO {clean_table_name(category, address_filter)} (NAME, LONGITUDE, LATITUDE, ADDRESS, RATING, RATING_COUNT, GOOGLE_TAGS, GOOGLE_URL, WARD, DISTRICT, CITY, PROVINCE, TYPE, SEARCH_ID, DATA_UPDATE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
                        #                 with sqlite3.connect(config['Data_source']['Local'].get('Location')) as connection:
                        #                     cursor = connection.cursor()
                        #                     cursor.executemany(query, values)
                        #             except Exception as e:
                        #                 logger.error(f'Writing to sqlite failed: {e}')
                        #                 print(f'{name}; {longitude}; {latitude}; {address}; {rating}; {rating_count}; {google_tag}; {google_url}; {ward}; {district}; {city}; {province}; {category}; {search_id}')
                        #                 raise
                    
                        #         elif database_type.lower() == 'mariadb':
                        #             try:
                        #                 query = f'INSERT INTO {clean_table_name(category, address_filter)} (NAME, LONGITUDE, LATITUDE, ADDRESS, RATING, RATING_COUNT, GOOGLE_TAGS, GOOGLE_URL, WARD, DISTRICT, CITY, PROVINCE, TYPE, SEARCH_ID, DATA_UPDATE) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                        #                 host, port, user, password, database = [i.replace(' ','') for i in open('authentication/mariadb', 'r').read().split(',')]
                        #                 connection = pymysql.connect(host=host, port=int(port), user=user, password=password, database=database, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
                        
                        #                 try:
                        #                     cursor = connection.cursor()
                        #                     cursor.executemany(query, values)
                        #                     connection.commit()
                        #                 except Exception as e:
                        #                     logger.error(e)
                        #                 finally:
                        #                     connection.close()
                        #             except Exception as e:
                        #                 logger.error(f'Writing to mariadb failed: {e}')
                        #                 print(name, longitude, latitude, address, rating, rating_count, google_tag, google_url, ward, district, city, province, category, search_id)
                        #                 raise
                    
                        #         else:
                        #             logger.error('[ERROR] Database type not recognized, please check if the config.yml is correct.')
                        #             # bar()

                        #         logger.info(f'[SUCCESS] Query {i+1}/{len(df_search)} {a} data in {ward}, {district}, {city}, {province}')
                        #         # bar()

                        #     else:
                        #         logger.info(f'[EMPTY] Query {i+1}/{len(df_search)} 0 data in {ward}, {district}, {city}, {province}')
                        #         # bar()
                        # except Exception as e:
                        #     logger.error(e)
                        
                        ################# ASYNC
                        # try:
                        #     is_tor = config.get('Is_tor')
                        # except:
                        #     is_tor = ''

                        if config.get('Detailed_search_proxy') == True or config.get('Detailed_search_proxy') is None:
                            results = asyncio.run(main(targets_no_ad, proxy_detail, ward, district, city, province, category, search_id, dbtime))
                        elif config.get('Detailed_search_proxy') == False:
                            results = asyncio.run(main(targets_no_ad, '', ward, district, city, province, category, search_id, dbtime))
                        else:
                            logger.error('Unrecognized detailed search proxy configuration')
                            raise
                        
                        # if results:
                        #     if database_type.lower() == 'sqlite':
                        #         db_location = config['Data_source']['Local'].get('Location')
                        #         asyncio.run(write_to_sqlite(db_location, table_name, results))
                        #     elif database_type.lower() == 'mariadb':
                        #         host, port, user, password, database = [i.replace(' ','') for i in open('authentication/mariadb', 'r').read().split(',')]
                        #         asyncio.run(write_to_mariadb(host, port, user, password, database, table_name, results))
                        #     else:
                        #         logger.error('[ERROR] Database type not recognized, please check if the config.yml is correct.')

                        try:
                            if results:
                                if database_type.lower() == 'sqlite':
                                    try:
                                        query = f'INSERT INTO {clean_table_name(category, address_filter)} (NAME, LONGITUDE, LATITUDE, ADDRESS, RATING, RATING_COUNT, GOOGLE_TAGS, GOOGLE_URL, WARD, DISTRICT, CITY, PROVINCE, TYPE, SEARCH_ID, DATA_UPDATE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
                                        with sqlite3.connect(config['Data_source']['Local'].get('Location')) as connection:
                                            cursor = connection.cursor()
                                            cursor.executemany(query, results)
                                    except Exception as e:
                                        logger.error(f'Writing to sqlite failed: {e}')
                                        # print(f'{name}; {longitude}; {latitude}; {address}; {rating}; {rating_count}; {google_tag}; {google_url}; {ward}; {district}; {city}; {province}; {category}; {search_id}')
                                        raise
                    
                                elif database_type.lower() == 'mariadb':
                                    try:
                                        query = f'INSERT INTO {clean_table_name(category, address_filter)} (NAME, LONGITUDE, LATITUDE, ADDRESS, RATING, RATING_COUNT, GOOGLE_TAGS, GOOGLE_URL, WARD, DISTRICT, CITY, PROVINCE, TYPE, SEARCH_ID, DATA_UPDATE) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                                        host, port, user, password, database = [i.replace(' ','') for i in open('authentication/mariadb', 'r').read().split(',')]
                                        connection = pymysql.connect(host=host, port=int(port), user=user, password=password, database=database, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
                        
                                        try:
                                            cursor = connection.cursor()
                                            cursor.executemany(query, results)
                                            connection.commit()
                                        except Exception as e:
                                            logger.error(e)
                                        finally:
                                            connection.close()
                                    except Exception as e:
                                        logger.error(f'Writing to mariadb failed: {e}')
                                        # print(name, longitude, latitude, address, rating, rating_count, google_tag, google_url, ward, district, city, province, category, search_id)
                                        raise
                    
                                else:
                                    logger.error('[ERROR] Database type not recognized, please check if the config.yml is correct.')
                                    # bar()

                                logger.info(f'[SUCCESS] Query {i+1}/{len(df_search)} {len(results)} data in {ward}, {district}, {city}, {province}')
                                # bar()

                            else:
                                logger.info(f'[EMPTY] Query {i+1}/{len(df_search)} 0 data in {ward}, {district}, {city}, {province}')
                                # bar()
                        except Exception as e:
                            logger.error(e)

                        # logger.info(f'[SUCCESS] Query {i+1}/{len(df_search)} {len(results)} data in {ward}, {district}, {city}, {province}')

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