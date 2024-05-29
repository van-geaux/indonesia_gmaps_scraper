import pandas as pd
import pymysql
import sqlite3
import urllib.parse

from src.backend import *

# logging.basicConfig(filename='error.log', level=logging.ERROR)


def create_new_df_cari(database_type, jenis, filter_wilayah=''):
    table_name = clean_table_name(jenis, filter_wilayah)
    
    if database_type.lower() == 'sqlite':
        try:
            with sqlite3.connect('backend/data.db') as connection:
                cursor = connection.cursor()
                cursor.execute(f'SELECT IDCARI FROM {table_name} ORDER BY ID DESC LIMIT 1')
                last_cari = cursor.fetchone()[0]
        except Exception:
            last_cari = 0

        query = f'SELECT PROPINSI, KOTA, KECAMATAN, KELURAHAN, KODEPOS, ID AS IDCARI FROM randomized_pos WHERE IDCARI > {last_cari}'
        if filter_wilayah:
            propinsi = filter_wilayah['PROPINSI']
            kota = filter_wilayah['KOTA']
            kecamatan = filter_wilayah['KECAMATAN']
            kelurahan = filter_wilayah['KELURAHAN']

        if propinsi:
            query += f' AND PROPINSI = "{propinsi}"'
        if kota:
            query += f' AND KOTA = "{kota}"'
        if kecamatan:
            query += f' AND KECAMATAN = "{kecamatan}"'
        if kelurahan:
            query += f' AND KELURAHAN = "{kelurahan}"'

        with sqlite3.connect('backend/data.db') as connection:
            df_cari = pd.DataFrame(pd.read_sql_query(query, connection))
        
        return df_cari
    
    elif database_type.lower() == 'mariadb':
        host, port, user, password, database = [i.replace(' ','') for i in open('authentication/mariadb', 'r').read().split(',')]
        connection = pymysql.connect(host=host, port=int(port), user=user, password=password, database=database, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                try:
                    cursor.execute(f'SELECT IDCARI FROM {table_name} ORDER BY ID DESC LIMIT 1')
                    last_cari = cursor.fetchone()[0]
                except Exception:
                    last_cari = 0

                query = f'SELECT PROPINSI, KOTA, KECAMATAN, KELURAHAN, KODEPOS, ID AS IDCARI FROM randomized_pos WHERE ID > {last_cari}'
                if filter_wilayah:
                    propinsi = filter_wilayah['PROPINSI']
                    kota = filter_wilayah['KOTA']
                    kecamatan = filter_wilayah['KECAMATAN']
                    kelurahan = filter_wilayah['KELURAHAN']

                if propinsi:
                    query += f' AND PROPINSI = "{propinsi}"'
                if kota:
                    query += f' AND KOTA = "{kota}"'
                if kecamatan:
                    query += f' AND KECAMATAN = "{kecamatan}"'
                if kelurahan:
                    query += f' AND KELURAHAN = "{kelurahan}"'

                cursor.execute(query)
                rows = cursor.fetchall()
                df_cari = pd.DataFrame(rows)
        except Exception as e:
            print(e)

        finally:
            connection.close()
            
        return df_cari
    
def remove_spaces(input_string):
    result_string = input_string.replace(" ", "")
    return result_string

def create_search_link(query: str, lang, geo_coordinates, zoom):
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

    return url

def proxy_auth(proxy_name):
    user, password, domain = [i.replace(' ','') for i in open(f'authentication/{proxy_name}', 'r').read().split(',')]
    return user, password, domain