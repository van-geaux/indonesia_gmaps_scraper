from alive_progress import alive_bar
from bs4 import BeautifulSoup
from datetime import datetime

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from src.backend import db_insert
from src.driver import get_driver, get_tor_driver, get_driver_extension
from src.logger import logger
from src.utilities import clean_table_name, create_new_df_search, create_search_link

import aiohttp
import asyncio
import json
import pymysql
import re
import sqlite3
import sys
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

async def renew_tor_ip(tor_password):
    try:
        with Controller.from_port(port=9051) as controller:
            controller.authenticate(password=tor_password)
            controller.signal(Signal.NEWNYM)
        await asyncio.sleep(5)
    except Exception as e:
        logger.error(f"Error renewing Tor IP: {e}")
        raise

async def fetch(session, google_url, proxy_detail, name, retries=10):
    timeout = ClientTimeout(total=10)
    for attempt in range(retries+2):
        try:
            async with session.get(google_url, proxy=proxy_detail, ssl=False, timeout=timeout) as response:
                if attempt > retries:
                    logger.warning(f'All 10 attempts failed for "{name}"')
                    logger.warning(f'Please check network connectivity')
                    sys.exit(1)

                response.raise_for_status()

                if attempt > 0:
                    logger.info(f'Retry attempt succeeded for "{name}"')

                return await response.text()
            
        except (ClientError, ClientConnectorError) as e:
            logger.info(f'Request failed for "{name}": {e}')
            logger.info(f'Retry attempt {attempt + 1} for "{name}"...')
            await asyncio.sleep(1)
        except Exception as e:
            logger.error(f'An unexpected error occurred for "{name}": {e}')
            logger.info(f'Retry attempt {attempt + 1} for "{name}"...')
            await asyncio.sleep(1)
    return None

async def fetch_tor(google_url, name, tor_driver=None, retries=4):
    timeout = ClientTimeout(total=10)
    connector = ProxyConnector.from_url('socks5://localhost:9150')
    cookie_jar = aiohttp.CookieJar()

    for attempt in range(retries):
        try:
            # async with aiohttp.ClientSession(connector=connector, cookie_jar=cookie_jar) as session:
            #     async with session.get(google_url, ssl=False, timeout=timeout) as response:
            #         if attempt > 3:
            #             logger.warning(f'All attempts failed for "{name}"')
            #             return None

            #         response.raise_for_status()
                    
            #         if attempt > 0:
            #             logger.info(f'Retry attempt succeeded for "{name}"')

            #         response_text  = await response.text()

            #         if 'Sebelum Anda melanjutkan ke Google' in response_text or 'Before you continue to Google' in response_text:
            #             logger.warning('Please open google map on tor and accept their cookies')
            #             status = 'No cookies'
            #             raise

            #         return response_text

            if attempt > 3:
                logger.warning(f'All attempts failed for "{name}"')

            response_text = tor_driver.get(google_url)

            if attempt > 0:
                logger.info(f'Rety attempt succeeded for "{name}"')

            return response_text
            
        except (ClientError, ClientConnectorError) as e:
            logger.info(f'Request failed for "{name}": {e}')
            logger.info(f'Retry attempt {attempt + 1} for "{name}"...')
            await asyncio.sleep(1)
        except Exception as e:
            # if status == 'No cookies':
            #     raise
            logger.error(f'An unexpected error occurred for "{name}": {e}')
            logger.info(f'Retry attempt {attempt + 1} for "{name}"...')
            await asyncio.sleep(1)
    return None

async def process_target(session, target, proxy_detail, ward, district, city, province, category, search_id, dbtime, tor_driver=None, renewal_task=None):
    try:
        name = target.find_all("div", {'class':True})[0].find('a')['aria-label']
        if name == "..":
            return ("..", "..", "..", "..", 0, 0, "..", "..", ward, district, city, province, category, search_id, dbtime)

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

            if renewal_task is not None:
                logger.debug('Using tor proxy')
                search_data_deep = await fetch_tor(google_url, name, tor_driver)
            else:
                logger.debug('Using selenium driver')
                search_data_deep = await fetch(session, google_url, proxy_detail, name)

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

async def main(targets_no_ad, proxy_detail, ward, district, city, province, category, search_id, dbtime, tor_driver=None, renewal_task=None):
    try:
        async with aiohttp.ClientSession() as session:
            tasks = []
            for i in range(0, len(targets_no_ad)):
                target = targets_no_ad[i]

                if renewal_task is not None:
                    task = process_target(session, target, proxy_detail, ward, district, city, province, category, search_id, dbtime, tor_driver, renewal_task,)
                else:
                    task = process_target(session, target, proxy_detail, ward, district, city, province, category, search_id, dbtime)
                
                tasks.append(task)
            results = await asyncio.gather(*tasks)
        return [result for result in results if result is not None]
    except Exception as e:
        logger.error(e)
        raise

async def parent_query(i, driver, loglevel, df_search, ward, district, city, province, search_url, bar, renewal_task, start_time):
    try:
        if renewal_task:
            renewal_task = asyncio.create_task(renew_tor_ip('Triplezeta17'))
        else:
            renewal_task = None
    except:
        renewal_task = None
                    
    try:
        logger.debug(f'Checking proxy availability')
        try:
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
        targets_no_ad = 'break'
        return targets_no_ad, proxy_check
                    
    try:
        divSideBar=driver.find_element(By.CSS_SELECTOR, "div[role='feed']")
    except Exception as e:
        logger.info(f'[EMPTY] Query {i+1}/{len(df_search)} 0 data in {ward}, {district}, {city}, {province}')
        logger.info(f'Total time {time.time() - start_time}')
        print('')
        bar()
        targets_no_ad = 'continue'
        return targets_no_ad, proxy_check
                    
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
        targets_no_ad = None

    if renewal_task is not None:
        await renewal_task
        logger.debug('Tor IP refreshed')
    
    return targets_no_ad, proxy_check

def deep_scraper(config):
    try:
        logger.debug(f'Getting configuration')
        database_type = ('sqlite' if not config['Data_source']['External'].get('Type').lower() else config['Data_source']['External'].get('Type').lower())
        category = config.get('Category')
        address_filter = config['Address_level']

        if config.get('Detailed_search_proxy') == True or config.get('Detailed_search_proxy') is None:
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
            if config.get('Scrape_address') == 'loop':
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
            if config.get('Is_tor'):
                tor_driver = get_tor_driver(config)
        except:
            pass

        try:
            with alive_bar(calibrate=15, enrich_print=False) as bar:
                for i in range(len(df_search)):
                # for i, r in df_search.iterrows():
                    start_time = time.time()

                    try:
                        if config.get('Is_tor'):
                            renewal_task = 'renew'
                        else:
                            renewal_task = None
                    except:
                        renewal_task = None

                    try:
                        logger.debug(f'Setting search query from database')
                        dbtime= datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        province = df_search.iloc[i].iloc[0]
                        city = df_search.iloc[i].iloc[1]
                        district = df_search.iloc[i].iloc[2]
                        ward = df_search.iloc[i].iloc[3]
                        search_id = int(df_search.iloc[i].iloc[5])
                        query = f'{category} in {ward}, {district}, {city}, {province}'
                        search_url = create_search_link(query, None, '', 18)
                        loglevel = config.get('Log_level')
                    except Exception as e:
                        logger.error(f'Setting search query from database failed: {e}')
                        raise

                    targets_no_ad, proxy_check = asyncio.run(parent_query(i, driver, loglevel, df_search, ward, district, city, province, search_url, bar, renewal_task, start_time))

                    if targets_no_ad == 'continue':
                        continue
                    elif targets_no_ad == 'break':
                        break
                    elif targets_no_ad is not None:
                        
                        if renewal_task is not None:
                            results = asyncio.run(main(targets_no_ad, proxy_detail, ward, district, city, province, category, search_id, dbtime, tor_driver, renewal_task))
                        else:
                            results = asyncio.run(main(targets_no_ad, proxy_detail, ward, district, city, province, category, search_id, dbtime))

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
                                        host = config['Data_source']['External'].get('Domain')
                                        port = config['Data_source']['External'].get('Port')
                                        user = config['Data_source']['External'].get('User')
                                        password = config['Data_source']['External'].get('Password')
                                        database = config['Data_source']['External'].get('Database_name')
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
                try:
                    tor_driver.close()
                except:
                    pass
            except Exception:
                pass

        if i+1 == len(df_search):
            break

    if proxy_count > 10 and proxy_check == 'Proxy failed':    
        logger.warning(f'Proxies failed for {proxy_count} times, exiting script.')
        
    logger.info(f'"{category}" scrape finished')