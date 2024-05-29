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
from src.utilities import *

import json
import pymysql
import re
import requests
import sqlite3
import time

# logging.basicConfig(filename='error.log', level=logging.ERROR)

def map_scraper(database_type, jenis, filter_wilayah, proxy=''):
    if proxy:
        user, password, domain = proxy_auth('proxyscrape')
        proxy_insert = f"{user}:{password}@{domain}"
        proxy_detail = {
                "https":f"http://{proxy_insert}"
            }

    df_cari = create_new_df_cari(database_type, jenis, filter_wilayah)

    query = f'INSERT INTO {clean_table_name(jenis, filter_wilayah)} (NAMA, KOORDINAT, ALAMAT, RATING, JML_RATING, TAG_GOOGLE, KELURAHAN, KECAMATAN, KOTA, PROVINSI, TIPE, IDCARI, DATA_UPDATE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

    for i in range(len(df_cari)):
        start_time = time.time()
        dbtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        propinsi = df_cari.iloc[i].iloc[0]
        kota = df_cari.iloc[i].iloc[1]
        kecamatan = df_cari.iloc[i].iloc[2]
        kelurahan = df_cari.iloc[i].iloc[3]
        idcari = int(df_cari.iloc[i].iloc[5])
        cari = f'{jenis} in {kelurahan}, {kecamatan}, {kota}, {propinsi}'
        url_cari = create_search_link(cari, None, '', 18)
    
        if proxy:
            retry_count = 0
            while retry_count <= 10:
                try:
                    response = requests.get(url_cari, proxies=proxy_detail)
                    if response.status_code == 200:
                        break
                except Exception as e:
                    print(e)
                    print('Proxy gagal, mencoba proxy lain')
                    pass
        
                retry_count += 1
            
            if retry_count > 60:
                print('Seluruh proxy gagal')
                break
        else:
            response = requests.get(url_cari)
    
        data_cari = response.text
        soup_cari = BeautifulSoup(data_cari, 'html.parser')
        scripts = soup_cari.find_all('script')
        values = []
    
        for script in scripts:
            if 'window.APP_INITIALIZATION_STATE' in str(script):
                data = str(script).split('=',3)[3]
                data2 = data.rsplit(';',10)[0]
                json_data = json.loads(data2)
                usaha = json_data[3][2][5:]
                json_usaha = json.loads(usaha)
                a = 1
                while True:
                    try:
                        nama = json_usaha[0][1][a][-1][11]
                        koordinat = ', '.join(list(map(str, json_usaha[0][1][a][-1][9][-2:])))
                        alamat = ', '.join(json_usaha[0][1][a][-1][2])
                        tag_google = ', '.join(json_usaha[0][1][a][-1][13])

                        try:
                            rating = float(json_usaha[0][1][a][-1][4][-2])
                        except:
                            rating = 0 

                        try:
                            jml_rating = int(json_usaha[0][1][a][-1][4][-1])
                        except:
                            jml_rating = 0
                            
                        values.append((nama, koordinat, alamat, rating, jml_rating, tag_google, kelurahan, kecamatan, kota, propinsi, jenis, idcari, dbtime))
                        a += 1

                    except:
                        break
            break
        
        if values:
            if database_type.lower() == 'sqlite':
                query = f'INSERT INTO {clean_table_name(jenis, filter_wilayah)} (NAMA, KOORDINAT, ALAMAT, RATING, JML_RATING, TAG_GOOGLE, KELURAHAN, KECAMATAN, KOTA, PROVINSI, TIPE, IDCARI, DATA_UPDATE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
                with sqlite3.connect('backend/data.db') as connection:
                    cursor = connection.cursor()
                    cursor.executemany(query, values)

            elif database_type.lower() == 'mariadb':
                query = f'INSERT INTO {clean_table_name(jenis, filter_wilayah)} (NAMA, KOORDINAT, ALAMAT, RATING, JML_RATING, TAG_GOOGLE, KELURAHAN, KECAMATAN, KOTA, PROVINSI, TIPE, IDCARI, DATA_UPDATE) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                host, port, user, password, database = [i.replace(' ','') for i in open('authentication/mariadb', 'r').read().split(',')]
                connection = pymysql.connect(host=host, port=int(port), user=user, password=password, database=database, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

                try:
                    with connection.cursor() as cursor:
                        cursor = connection.cursor()
                        cursor.executemany(query, values)
                        connection.commit()
                except Exception as e:
                    print(e)
                finally:
                    connection.close()                     

            else:
                print('Database tidak dikenal')
                                        
        print(f'Query {i+1}/{len(df_cari)} {jenis} di kelurahan {kelurahan} kecamatan {kecamatan} kota {kota} provinsi {propinsi} selesai diinput sebanyak {a-1} data')
        print(f'Total waktu query {time.time() - start_time}')
    
    print(f'Scrape {jenis} selesai')

def get_driver(proxy=None):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920x1080")

    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("--silent")

    capabilities = webdriver.DesiredCapabilities.CHROME
    
    if proxy:
        user, password, domain = proxy_auth('proxyscrape')
        proxy_insert = f"{user}:{password}@{domain}"

        prox = Proxy()
        prox.proxy_type = ProxyType.MANUAL
        prox.ssl_proxy = f"http://{proxy_insert}"

        prox.add_to_capabilities(capabilities)

    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options, desired_capabilities=capabilities)
    except Exception:
        driver = webdriver.Chrome(service=Service('driver/124.0.6367.207/chromedriver-win32/chromedriver.exe'), options=chrome_options, desired_capabilities=capabilities)

    return driver

def map_scraper_with_scrolls(database_type, jenis, filter_wilayah, proxy):
    proxy_count = 0
    cek_proxy = ''

    while proxy_count < 10:
        df_cari = create_new_df_cari(database_type, jenis, filter_wilayah)
        print(f'Ekspektasi jumlah query di cycle ini: {len(df_cari)}')

        driver = get_driver(proxy)

        try:
            for i in range(len(df_cari)):
                start_time = time.time()
                dbtime= datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                propinsi = df_cari.iloc[i].iloc[0]
                kota = df_cari.iloc[i].iloc[1]
                kecamatan = df_cari.iloc[i].iloc[2]
                kelurahan = df_cari.iloc[i].iloc[3]
                idcari = int(df_cari.iloc[i].iloc[5])
                cari = f'{jenis} in {kelurahan}, {kecamatan}, {kota}, {propinsi}'
                url_cari = create_search_link(cari, None, '', 18)
                
                driver.get(url_cari)

                try:
                    WebDriverWait(driver, 10).until(EC.title_contains("Google Maps"))
                    cek_proxy = ''
                except Exception:
                    cek_proxy = 'Proxy gagal'
                    break

                try:
                    divSideBar=driver.find_element(By.CSS_SELECTOR, "div[role='feed']")
                except Exception:
                    print(f'Query {i+1}/{len(df_cari)} kosong kelurahan {kelurahan} kecamatan {kecamatan} kota {kota} provinsi {propinsi}')
                    print(f'Total waktu {time.time() - start_time}')
                    continue

                keepScrolling=True
                while keepScrolling:
                    divSideBar.send_keys(Keys.PAGE_DOWN)
                    div_html = driver.find_element(By.TAG_NAME, "html").get_attribute('outerHTML')

                    if "You've reached the end of the list." in div_html or 'Anda telah mencapai akhir daftar.' in div_html:
                        keepScrolling=False

                soup_cari = BeautifulSoup(driver.page_source, 'html.parser')
                targets = soup_cari.find("div", {'role': 'feed'}).find_all('div', {'class': False})[:-1]
                targets_no_ad = [div for div in targets if div.find('div', {'jsaction':True})]

                values = []
                a = 0
                while True:
                    try:
                        nama = targets_no_ad[a].find_all("div", {'class':True})[0].find('a')['aria-label']
                        coordinate = re.search(r'!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)', targets_no_ad[a].find_all("div")[0].find("a")['href'])
                        longlat = f'{coordinate.group(1)}, {coordinate.group(2)}'
                        alamat = [span for span in targets_no_ad[a].find_all('span', {'aria-hidden':'', 'aria-label':'', 'class':''}) if not span.find('span')][1].text.strip()
                        rating = float(targets_no_ad[a].find_all('span')[4].find_all('span')[0].text.strip().replace(',','.'))

                        try:
                            jml_rating = int(targets_no_ad[a].find_all("div")[17].find_all("span")[4].text.strip()[1:-1].replace(',',''))
                        except:
                            jml_rating = 0

                        try:
                            tag_google = [span for span in targets_no_ad[a].find_all('span', {'aria-label':'', 'aria-hidden':'', 'class':''}) if not span.find('span')][0].text.strip()
                        except:
                            tag_google = ''

                        values.append((nama, longlat, alamat, rating, jml_rating, tag_google, kelurahan, kecamatan, kota, propinsi, jenis, idcari, dbtime))
                        a += 1
                    except Exception:
                        break

                if values:
                    if database_type.lower() == 'sqlite':
                        query = f'INSERT INTO {clean_table_name(jenis, filter_wilayah)} (NAMA, KOORDINAT, ALAMAT, RATING, JML_RATING, TAG_GOOGLE, KELURAHAN, KECAMATAN, KOTA, PROVINSI, TIPE, IDCARI, DATA_UPDATE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
                        with sqlite3.connect('backend/data.db') as connection:
                            cursor = connection.cursor()
                            cursor.executemany(query, values)
        
                    elif database_type.lower() == 'mariadb':
                        query = f'INSERT INTO {clean_table_name(jenis, filter_wilayah)} (NAMA, KOORDINAT, ALAMAT, RATING, JML_RATING, TAG_GOOGLE, KELURAHAN, KECAMATAN, KOTA, PROVINSI, TIPE, IDCARI, DATA_UPDATE) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                        host, port, user, password, database = [i.replace(' ','') for i in open('authentication/mariadb', 'r').read().split(',')]
                        connection = pymysql.connect(host=host, port=int(port), user=user, password=password, database=database, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        
                        try:
                            with connection.cursor() as cursor:
                                cursor = connection.cursor()
                                cursor.executemany(query, values)
                                connection.commit()
                        except Exception as e:
                            print(e)
                        finally:
                            connection.close()                     
        
                    else:
                        print('Database tidak dikenal')

                print(f'Query {i+1}/{len(df_cari)} {jenis} di kelurahan {kelurahan} kecamatan {kecamatan} kota {kota} provinsi {propinsi} selesai diinput sebanyak {a+1} data')
                print(f'Total waktu {time.time() - start_time}')
        
        finally:
            proxy_count += 1
            try:
                driver.close()
            except Exception:
                pass

        if i+1 == len(df_cari):
            break

    if proxy_count > 10 and cek_proxy == 'Proxy gagal':    
        status = 'Seluruh proxy gagal'
        print(status)

    status = f'Scrape {jenis} selesai'
        
    print(status)

def map_scraper_with_scrolls_deep(database_type, jenis, filter_wilayah, proxy):
    proxy_count = 0
    cek_proxy = ''
    user, password, domain = proxy_auth('proxyscrape')
    proxy_insert = f"{user}:{password}@{domain}"
    proxy_detail = {
            "https":f"http://{proxy_insert}"
        }

    while proxy_count < 10:
        df_cari = create_new_df_cari(database_type, jenis, filter_wilayah)
        print(f'Ekspektasi jumlah query di cycle ini: {len(df_cari)}')

        driver = get_driver(proxy)

        try:
            for i in range(len(df_cari)):
                start_time = time.time()
                dbtime= datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                propinsi = df_cari.iloc[i].iloc[0]
                kota = df_cari.iloc[i].iloc[1]
                kecamatan = df_cari.iloc[i].iloc[2]
                kelurahan = df_cari.iloc[i].iloc[3]
                idcari = int(df_cari.iloc[i].iloc[5])
                cari = f'{jenis} in {kelurahan}, {kecamatan}, {kota}, {propinsi}'
                url_cari = create_search_link(cari, None, '', 18)
                
                driver.get(url_cari)

                try:
                    WebDriverWait(driver, 10).until(EC.title_contains("Google Maps"))
                    cek_proxy = ''
                except Exception:
                    cek_proxy = 'Proxy gagal'
                    break

                try:
                    divSideBar=driver.find_element(By.CSS_SELECTOR, "div[role='feed']")
                except Exception:
                    print(f'Query {i+1}/{len(df_cari)} kosong kelurahan {kelurahan} kecamatan {kecamatan} kota {kota} provinsi {propinsi}')
                    print(f'Total waktu {time.time() - start_time}')
                    continue

                keepScrolling=True
                while keepScrolling:
                    divSideBar.send_keys(Keys.PAGE_DOWN)
                    div_html = driver.find_element(By.TAG_NAME, "html").get_attribute('outerHTML')

                    if "You've reached the end of the list." in div_html or 'Anda telah mencapai akhir daftar.' in div_html:
                        keepScrolling=False

                soup_cari = BeautifulSoup(driver.page_source, 'html.parser')
                targets = soup_cari.find("div", {'role': 'feed'}).find_all('div', {'class': False})[:-1]
                targets_no_ad = [div for div in targets if div.find('div', {'jsaction':True})]

                values = []
                a = 0
                while True: 
                    try:
                        url_cari = targets_no_ad[a].find_all('a')[0]['href']

                        if proxy:
                            response = requests.get(url_cari, proxies=proxy_detail)
                        else:
                            response = requests.get(url_cari)

                        data_cari_depth = response.text
                        soup_cari_depth = BeautifulSoup(data_cari_depth, 'html.parser')
                        scripts_depth = soup_cari_depth.find_all('script')

                        for script_depth in scripts_depth:
                            if 'window.APP_INITIALIZATION_STATE' in str(script_depth):
                                data_depth = str(script_depth).split('=',3)[3]
                                data2_depth = data_depth.rsplit(';',10)[0]
                                json_data_depth = json.loads(data2_depth)
                                usaha_depth = json_data_depth[3][-1][5:]
                                json_usaha_depth = json.loads(usaha_depth)
                                break

                        nama = json_usaha_depth[6][11]
                        alamat = ', '.join(json_usaha_depth[6][2])
                        rating = float(json_usaha_depth[6][4][7])
                        jml_rating = int(json_usaha_depth[6][4][8])
                        longlat = ', '.join(str(k) for k in json_usaha_depth[6][9][-2:])
                        tag_google = ', '.join(json_usaha_depth[6][13])

                        values.append((nama, longlat, alamat, rating, jml_rating, tag_google, kelurahan, kecamatan, kota, propinsi, jenis, idcari, dbtime))
                        a += 1
                    except Exception:
                        break

                if values:
                    if database_type.lower() == 'sqlite':
                        query = f'INSERT INTO {clean_table_name(jenis, filter_wilayah)} (NAMA, KOORDINAT, ALAMAT, RATING, JML_RATING, TAG_GOOGLE, KELURAHAN, KECAMATAN, KOTA, PROVINSI, TIPE, IDCARI, DATA_UPDATE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
                        with sqlite3.connect('backend/data.db') as connection:
                            cursor = connection.cursor()
                            cursor.executemany(query, values)
        
                    elif database_type.lower() == 'mariadb':
                        query = f'INSERT INTO {clean_table_name(jenis, filter_wilayah)} (NAMA, KOORDINAT, ALAMAT, RATING, JML_RATING, TAG_GOOGLE, KELURAHAN, KECAMATAN, KOTA, PROVINSI, TIPE, IDCARI, DATA_UPDATE) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                        host, port, user, password, database = [i.replace(' ','') for i in open('authentication/mariadb', 'r').read().split(',')]
                        connection = pymysql.connect(host=host, port=int(port), user=user, password=password, database=database, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        
                        try:
                            with connection.cursor() as cursor:
                                cursor = connection.cursor()
                                cursor.executemany(query, values)
                                connection.commit()
                        except Exception as e:
                            print(e)
                        finally:
                            connection.close()                     
        
                    else:
                        print('Database tidak dikenal')

                print(f'Query {i+1}/{len(df_cari)} {jenis} di kelurahan {kelurahan} kecamatan {kecamatan} kota {kota} provinsi {propinsi} selesai diinput sebanyak {a} data')
                print(f'Total waktu {time.time() - start_time}')
        
        finally:
            proxy_count += 1
            try:
                driver.close()
            except Exception:
                pass

        if i+1 == len(df_cari):
            break

    if proxy_count > 10 and cek_proxy == 'Proxy gagal':    
        status = 'Seluruh proxy gagal'
        print(status)

    status = f'Scrape {jenis} selesai'
        
    print(status)