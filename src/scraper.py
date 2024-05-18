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

import json
import re
import requests
import sqlite3
import time

from src.backend import create_new_df_cari
from src.utilities import create_search_link

def map_scraper(jenis, jenis_table, df_cari):
    # proxyscrape.com
    username = "dl0kskmfsl8ssvi"
    password = "x2z4c0y1fqnvm15"
    proxy = "172.65.64.100:6060"
    proxy_auth = "{}:{}@{}".format(username, password, proxy)

    for i in range(0, len(df_cari)):
        total_time = time.time()
        provinsi = df_cari.iloc[i].iloc[0]
        kota = df_cari.iloc[i].iloc[1]
        kecamatan = df_cari.iloc[i].iloc[2]
        kelurahan = df_cari.iloc[i].iloc[3]
        idcari = int(df_cari.iloc[i].iloc[5])
        cari = f'{jenis} in {kelurahan}, {kecamatan}, {kota}, {provinsi}'
        url_cari = create_search_link(cari, None, '', 18)
    
        retry_count = 0
        while retry_count <= 60:
            try:
                proxy = {
                        "https":"http://{}".format(proxy_auth)
                    }
                response = requests.get(url_cari, proxies=proxy)
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
    
        data_cari = response.text
        soup_cari = BeautifulSoup(data_cari, 'html.parser')
        scripts = soup_cari.find_all('script')
    
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
                        nama = json_usaha[0][1][a][14][11]
                        koordinat = ', '.join(list(map(str, json_usaha[0][1][a][14][9][-2:])))
                        alamat = ', '.join(json_usaha[0][1][a][14][2])
                        try:
                            rating = json_usaha[0][1][a][14][4][3][1]
                            index_of_space = rating.find(" ")
                            rating_int = int(rating[:index_of_space])
                        except Exception:
                            rating_int = 0
                        updatetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        
                        try:
                            with sqlite3.connect('backend/data.db') as connection:
                                cursor = connection.cursor()
                                query = f'INSERT INTO {jenis_table} (NAMA, KOORDINAT, JML_RATING, ALAMAT, KELURAHAN, KECAMATAN, KOTA, PROVINSI, TIPE, IDCARI, DATA_UPDATE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
                                params = (nama, koordinat, rating_int, alamat, kelurahan, kecamatan, kota, provinsi, jenis, idcari, updatetime)
                                cursor.execute(query, params)

                        except Exception as e:
                            print(f'Error occurred: {str(e)} on kelurahan {kelurahan} kecamatan {kecamatan} kota {kota} provinsi {provinsi} index {a}')
                    
                        a += 1

                    except Exception:
                        break
                    
        print(f'{jenis} di kelurahan {kelurahan} kecamatan {kecamatan} kota {kota} provinsi {provinsi} selesai diinput sebanyak {a-1} data')
        print(f'Total waktu query {time.time() - total_time}')
    
    print(f'Scrape {jenis} selesai')

def get_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("--silent")
    
    # proxyscrape.com
    username = "dl0kskmfsl8ssvi"
    password = "x2z4c0y1fqnvm15"
    proxy = "rp.proxyscrape.com:6060"
    proxy_auth = "{}:{}@{}".format(username, password, proxy)

    prox = Proxy()
    prox.proxy_type = ProxyType.MANUAL
    prox.ssl_proxy = "http://{}".format(proxy_auth)
    capabilities = webdriver.DesiredCapabilities.CHROME
    prox.add_to_capabilities(capabilities)
    # chrome_options.add_argument(f'--proxy-server={proxy_auth}')

    try:
        driver = webdriver.Chrome(service=Service('driver/124.0.6367.207/chromedriver-win32/chromedriver.exe'), options=chrome_options, desired_capabilities=capabilities)
    except Exception:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options, desired_capabilities=capabilities)

    return driver

def map_scraper_with_scrolls(jenis, jenis_table, filter_wilayah, driver):
    proxy_count = 0
    query_count = 0
    cek_proxy = ''

    while proxy_count < 61:
        if cek_proxy == 'Proxy gagal':
            get_driver()
            print('Proxy baru')
        
        try:
            df_cari = create_new_df_cari(jenis_table, filter_wilayah)
            print(f'Ekspektasi jumlah query di cycle ini: {len(df_cari)}')

            for i in range(0, len(df_cari)):
                total_time = time.time()
                provinsi = df_cari.iloc[i].iloc[0]
                kota = df_cari.iloc[i].iloc[1]
                kecamatan = df_cari.iloc[i].iloc[2]
                kelurahan = df_cari.iloc[i].iloc[3]
                idcari = int(df_cari.iloc[i].iloc[5])
                cari = f'{jenis} in {kelurahan}, {kecamatan}, {kota}, {provinsi}'
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
                    # time.sleep(1)
                    # divSideBar.click()
                except Exception:
                    query_count += 1
                    print(f'Query {query_count}/{len(df_cari)} kosong kelurahan {kelurahan} kecamatan {kecamatan} kota {kota} provinsi {provinsi}')
                    print(f'Total waktu {time.time() - total_time}')
                    continue

                # actions = ActionChains(driver)

                keepScrolling=True
                while keepScrolling:
                    # actions.move_to_element(divSideBar).send_keys(Keys.PAGE_DOWN).perform()
                    # div_html = divSideBar.get_attribute('outerHTML')
                    divSideBar.send_keys(Keys.PAGE_DOWN)
                    div_html = driver.find_element(By.TAG_NAME, "html").get_attribute('outerHTML')

                    if "You've reached the end of the list." in div_html or 'Anda telah mencapai akhir daftar.' in div_html:
                        keepScrolling=False

                soup_cari = BeautifulSoup(driver.page_source, 'html.parser')
                targets = soup_cari.find("div", {'role': 'feed'}).find_all('div', {'class': False})[:-1]
                targets_no_ad = [div for div in targets if div.find('div', {'jsaction':True})]

                a = 1
                while True:
                    try:
                        nama = targets_no_ad[a].find_all("div", {'class':True})[0].find('a')['aria-label']

                        try:
                            jml_rating = int(targets_no_ad[a].find_all("div")[17].find_all("span")[4].text.strip()[1:-1].replace(',',''))
                        except:
                            jml_rating = 0

                        alamat = targets_no_ad[a].find_all('span', {'aria-hidden':'', 'aria-label':'', 'class':''})[3].text.strip()

                        try:
                            tag_google = [span for span in targets_no_ad[a].find_all('span', {'aria-label':'', 'aria-hidden':'', 'class':''}) if not span.find('span')][0].text.strip()
                        except:
                            tag_google = ''

                        coordinate = re.search(r'!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)', targets_no_ad[a].find_all("div")[0].find("a")['href'])
                        longlat = f'{coordinate.group(1)}, {coordinate.group(2)}'
                        updatetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                        try:
                            with sqlite3.connect('backend/data.db') as connection:
                                cursor = connection.cursor()
                                query = f'INSERT INTO {jenis_table} (NAMA, KOORDINAT, JML_RATING, ALAMAT, TAG_GOOGLE, KELURAHAN, KECAMATAN, KOTA, PROVINSI, TIPE, IDCARI, DATA_UPDATE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
                                params = (nama, longlat, jml_rating, alamat, tag_google, kelurahan, kecamatan, kota, provinsi, jenis, idcari, updatetime)
                                cursor.execute(query, params)
                        except Exception as e:
                            print(f'Error occurred: {str(e)} on kelurahan {kelurahan} kecamatan {kecamatan} kota {kota} provinsi {provinsi} index {a}')

                        a += 1

                    except Exception:
                        break
                
                query_count += 1
                print(f'Query {query_count}/{len(df_cari)} {jenis} di kelurahan {kelurahan} kecamatan {kecamatan} kota {kota} provinsi {provinsi} selesai diinput sebanyak {a-1} data')
                print(f'Total waktu {time.time() - total_time}')

            if cek_proxy == 'Proxy gagal':
                proxy_count += 1
                driver.close()
                break

        except Exception:
            break

    if proxy_count > 60 and cek_proxy == 'Proxy gagal':    
        status = 'Seluruh proxy gagal'
        driver.close()
        print(status)

    status = f'Scrape {jenis} selesai'
    if cek_proxy != 'Proxy gagal':
        driver.close()
    print(status)