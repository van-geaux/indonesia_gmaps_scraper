import pandas as pd
import pymysql
import sqlite3
import urllib.parse

from src.backend import clean_table_name
from src.logger import logger

# logging.basicConfig(filename='error.log', level=logging.ERROR)


def create_new_df_search(config, database_type, category, address_filter=''):
    table_name = clean_table_name(category, address_filter)
    try:
        if database_type.lower() == 'sqlite':
            try:
                with sqlite3.connect(config['Data_source'].get('Local').get('Location')) as connection:
                    cursor = connection.cursor()
                    cursor.execute(f'SELECT SEARCH_ID FROM {table_name} ORDER BY ID DESC LIMIT 1')
                    last_search = cursor.fetchone()[0]
            except Exception:
                last_search = 0

            query = f"SELECT PROVINCE, CITY, DISTRICT, WARD, POSTAL_CODE, ID AS SEARCH_ID FROM {config['Data_source']['Local'].get('Address_table_name')} WHERE SEARCH_ID > {last_search}"
            
            try:
                province = address_filter.get('Province').upper()
            except:
                province = ''
            
            try:
                city = address_filter.get('City').upper()
            except:
                city = ''
            
            try:
                district = address_filter.get('District/subdistrict').upper()
            except:
                district = ''
            
            try:
                ward = address_filter.get('Ward/village').upper()
            except:
                ward = ''

            if province:
                query += f' AND PROVINCE LIKE "%{province}%"'
            if city:
                query += f' AND CITY LIKE "%{city}%"'
            if district:
                query += f' AND DISTRICT LIKE "%{district}%"'
            if ward:
                query += f' AND WARD LIKE "%{ward}%"'

            with sqlite3.connect(config['Data_source']['Local'].get('Location')) as connection:
                df_search = pd.DataFrame(pd.read_sql_query(query, connection))
        
            return df_search
        
        elif database_type.lower() == 'mariadb':
            host = config['Data_source']['External'].get('Domain')
            port = config['Data_source']['External'].get('Port')
            user = config['Data_source']['External'].get('User')
            password = config['Data_source']['External'].get('Password')
            database = config['Data_source']['External'].get('Database_name')
            connection = pymysql.connect(host=host, port=int(port), user=user, password=password, database=database, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

            try:
                with connection.cursor() as cursor:
                    try:
                        cursor.execute(f'SELECT SEARCH_ID FROM perusahaan ORDER BY SEARCH_ID DESC LIMIT 1')
                        result = cursor.fetchone()
                        last_search = result.get('SEARCH_ID')
                    except Exception:
                        last_search = 0

                    query = f"SELECT PROVINCE, CITY, DISTRICT, WARD, POSTAL_CODE, ID FROM {config['Data_source']['External'].get('Address_table_name')} WHERE ID > {last_search}"
                    
                    try:
                        province = address_filter.get('Province').upper()
                    except:
                        province = ''
                    
                    try:
                        city = address_filter.get('City').upper()
                    except:
                        city = ''
                    
                    try:
                        district = address_filter.get('District/subdistrict').upper()
                    except:
                        district = ''
                    
                    try:
                        ward = address_filter.get('Ward/village').upper()
                    except:
                        ward = ''

                    if province:
                        query += f' AND PROVINCE LIKE "%{province}%"'
                    if city:
                        query += f' AND CITY LIKE "%{city}%"'
                    if district:
                        query += f' AND DISTRICT LIKE "%{district}%"'
                    if ward:
                        query += f' AND WARD LIKE "%{ward}%"'

                    cursor.execute(query)
                    rows = cursor.fetchall()
                    df_search = pd.DataFrame(rows)
            except Exception as e:
                print(e)

            finally:
                connection.close()
    except Exception as e:
        logger.error(e)
        df_search = ''
            
    return df_search
    
def remove_spaces(input_string):
    result_string = input_string.replace(" ", "")
    return result_string

def create_search_link(query: str, lang, geo_coordinates, zoom):
    try:
        if geo_coordinates is None and zoom is not None:
            raise ValueError("geo_coordinates must be provided along with zoom")

        endpoint = urllib.parse.quote_plus(query)

        params = {'authuser': '0',
                'hl': lang,
                'entry': 'ttu',} if lang is not None else {'authuser': '0',
                                                            'entry': 'ttu',}
        
        geo_str = ''
        if geo_coordinates is not None:
            geo_coordinates = remove_spaces(geo_coordinates)
            if zoom is not None:
                geo_str = f'/@{geo_coordinates},{zoom}z'
            else:
                geo_str = f'/@{geo_coordinates}'

        url = f'https://www.google.com/maps/search/{endpoint}'
        if geo_str:
            url += geo_str
        url += f'?{urllib.parse.urlencode(params)}'
    except Exception as e:
        logger.error(f'Creating search link failed: {e}')

    return url