from datetime import datetime

import pandas as pd
import pymysql
import sqlite3
import tabula

# logging.basicConfig(filename='error.log', level=logging.ERROR)

def clean_table_name(category, address_filter=''):
    province = address_filter.get('Province')
    city = address_filter.get('City')
    district = address_filter.get('District/subdistrict')
    ward = address_filter.get('Ward/village')

    table_name = category.replace(' ', '')

    if province:
        table_name += f'_{province}'
    if city:
        table_name += f'_{city}'
    if district:
        table_name += f'_{district}'
    if ward:
        table_name += f'_{ward}'
        
    return table_name

def db_check(database_type, table_name, config):
    if database_type.lower() == 'sqlite':
        with sqlite3.connect(config['Data_source'].get('Local').get('Location')) as connection:
            cursor = connection.cursor()
            tables = {
                f"{table_name}": '"ID" INTEGER PRIMARY KEY NOT NULL, "NAME" TEXT, "LONGITUDE" TEXT, "LATITUDE" TEXT, "ADDRESS" TEXT, "RATING" REAL, "RATING_COUNT" INTEGER, "GOOGLE_TAGS" TEXT, "GOOGLE_URL" TEXT, "WARD" TEXT, "DISTRICT" TEXT, "CITY" TEXT, "PROVINCE" TEXT, "TYPE" TEXT, "SEARCH_ID" INTEGER, "DATA_UPDATE" DATETIME',
                "randomized_pos": '"ID" INTEGER PRIMARY KEY NOT NULL, "PROPINSI" TEXT, "KOTA" TEXT, "KECAMATAN" TEXT, "KELURAHAN" TEXT, "KODEPOS" TEXT, "DATA_UPDATE"'
            }
            for table, schema in tables.items():
                cursor.execute(f'CREATE TABLE IF NOT EXISTS {table} ({schema})')

    elif database_type.lower() == 'mariadb':
        host = config['Data_source']['External'].get('Domain')
        port = config['Data_source']['External'].get('Port')
        user = config['Data_source']['External'].get('User')
        password = config['Data_source']['External'].get('Password')
        database = config['Data_source']['External'].get('Database_name')
        connection = pymysql.connect(host=host, port=int(port), user=user, password=password, database=database, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                tables = {
                    f'{table_name}': 'ID INT AUTO_INCREMENT PRIMARY KEY, NAME TEXT, LONGITUDE TEXT, LATITUDE TEXT, ADDRESS TEXT, RATING FLOAT, RATING_COUNT INT, GOOGLE_TAGS TEXT, URL TEXT, WARD TEXT, DISTRICT TEXT, CITY TEXT, PROVINCE TEXT, TYPE TEXT, SEARCH_ID INT, DATA_UPDATE DATETIME',
                    'randomized_pos': 'ID INT AUTO_INCREMENT PRIMARY KEY, PROPINSI TEXT, KOTA TEXT, KECAMATAN TEXT, KELURAHAN TEXT, KODEPOS TEXT, DATA_UPDATE DATETIME'
                }
                for table, schema in tables.items():
                    cursor.execute(f'CREATE TABLE IF NOT EXISTS {table} ({schema})')
            connection.commit()
        finally:
            connection.close()

    else:
        print('Database not recognized')

def scrape_pos():
    with sqlite3.connect('backend/data.db') as connection:
        cursor = connection.cursor()

        tables = {
                "kode_pos": '"ID" INTEGER PRIMARY KEY NOT NULL, "PROPINSI" TEXT, "KOTA" TEXT, "KECAMATAN" TEXT, "KELURAHAN" TEXT, "KODEPOS" INTEGER, DATA_TIME DATETIME',
            }
        for table, schema in tables.items():
            cursor.execute(f'CREATE TABLE IF NOT EXISTS {table} ({schema})')
        
        dfs = tabula.read_pdf('backend/kodepos.pdf', pages='all')

        query = 'INSERT INTO kode_pos (NM_PROVINSI, NM_KABUPATEN, NM_KECAMATAN, NM_DESA, KODEPOS, DATA_TIME) VALUES (?, ?, ?, ?, ?, ?)'

        for i in range(len(dfs[0])):
            provinsi = dfs[0].iloc[i].iloc[0]
            kabupaten = dfs[0].iloc[i].iloc[1]
            kecamatan = dfs[0].iloc[i].iloc[2]
            desa = dfs[0].iloc[i].iloc[3]
            kodepos = int(dfs[0].iloc[i].iloc[4])

            params = (provinsi, kabupaten, kecamatan, desa, kodepos, datetime.now())
            cursor.execute(query, params)

        print('Halaman 1 kode pos selesai diimpor')

        for i in range(1,len(dfs)):
            new_row = pd.DataFrame([dfs[i].columns], columns=dfs[i].columns)
            df_transformed = pd.concat([new_row, dfs[i]], ignore_index=True)

            for a in range(len(df_transformed)):
                provinsi = df_transformed.iloc[a].iloc[0]
                kabupaten = df_transformed.iloc[a].iloc[1]
                kecamatan = df_transformed.iloc[a].iloc[2]
                desa = df_transformed.iloc[a].iloc[3]
                kodepos = int(df_transformed.iloc[a].iloc[4])

                params = (provinsi, kabupaten, kecamatan, desa, kodepos, datetime.now())
                cursor.execute(query, params)

            print(f'Halaman {i+1} selesai')

    print('\nSeluruh halaman kode pos selesai diimpor')

# cek database, kalau kosong isi randomized
def random_pos_check(database_type):
    def randomized_pos():
        with sqlite3.connect('./backend/data.db') as connection:
            query = 'SELECT * FROM kode_pos'
            df_cari = pd.read_sql_query(query, connection).sample(frac=1).reset_index(drop=True) # randomized order
            df_cari.fillna('', inplace=True)

        values = []
        for i in range (len(df_cari)):
            propinsi = df_cari.iloc[i]['PROPINSI']
            kota = df_cari.iloc[i]['KOTA']
            kecamatan = df_cari.iloc[i]['KECAMATAN']
            kelurahan = df_cari.iloc[i]['KELURAHAN']
            kodepos = df_cari.iloc[i]['KODE POS']
            update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            values.append((propinsi, kota, kecamatan, kelurahan, kodepos, update_time))

        return values

    if database_type.lower() == 'sqlite':
        with sqlite3.connect('./backend/data.db') as connection:
            cursor = connection.cursor()
            cursor.execute('SELECT COUNT(*) FROM randomized_pos')
            count = cursor.fetchone()[0]
            if count == 0:
                values = randomized_pos()
                query = ('INSERT INTO randomized_pos (PROPINSI, KOTA, KECAMATAN, KELURAHAN, KODEPOS, DATA_UPDATE) VALUES (?, ?, ?, ?, ?, ?)')
                cursor.executemany(query, values)

    elif database_type.lower() == 'mariadb':
        host, port, user, password, database = [i.replace(' ','') for i in open('authentication/mariadb', 'r').read().split(',')]
        connection = pymysql.connect(host=host, port=int(port), user=user, password=password, database=database, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                cursor.execute('SELECT COUNT(*) FROM randomized_pos')
                count = cursor.fetchone()['COUNT(*)']
                if count == 0:
                    values = randomized_pos()
                    query = ('INSERT INTO randomized_pos (PROPINSI, KOTA, KECAMATAN, KELURAHAN, KODEPOS, DATA_UPDATE) VALUES (%s, %s, %s, %s, %s, %s)')                 
                    cursor.executemany(query, values)
                
            connection.commit()

        finally:
            connection.close()

    else:
        print('Database tidak dikenal')