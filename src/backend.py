from datetime import datetime

import pandas as pd
import pymysql
import sqlite3

# logging.basicConfig(filename='error.log', level=logging.ERROR)

def clean_table_name(jenis, filter_wilayah=''):
    propinsi = filter_wilayah['PROPINSI'].replace(' ','').lower()
    kota = filter_wilayah['KOTA'].replace(' ','').lower()
    kecamatan = filter_wilayah['KECAMATAN'].replace(' ','').lower()
    kelurahan = filter_wilayah['KELURAHAN'].replace(' ','').lower()

    jenis_table = jenis.replace(' ', '')

    if propinsi:
        jenis_table += f'_{propinsi}'
    if kota:
        jenis_table += f'_{kota}'
    if kecamatan:
        jenis_table += f'_{kecamatan}'
    if kelurahan:
        jenis_table += f'_{kelurahan}'
        
    return jenis_table

def db_check(database_type, table_name):
    if database_type.lower() == 'sqlite':
        with sqlite3.connect("./backend/data.db") as connection:
            cursor = connection.cursor()
            tables = {
                f"{table_name}": '"ID" INTEGER PRIMARY KEY NOT NULL, "NAMA" TEXT, "KOORDINAT" TEXT, "ALAMAT" TEXT, "RATING" REAL, "JML_RATING" INTEGER, "TAG_GOOGLE" TEXT, "KELURAHAN" TEXT, "KECAMATAN" TEXT, "KOTA" TEXT, "PROVINSI" TEXT, "TIPE" TEXT, "IDCARI" INTEGER, "DATA_UPDATE" DATETIME',
                "randomized_pos": '"ID" INTEGER PRIMARY KEY NOT NULL, "PROPINSI" TEXT, "KOTA" TEXT, "KECAMATAN" TEXT, "KELURAHAN" TEXT, "KODEPOS" TEXT, "DATA_UPDATE"'
            }
            for table, schema in tables.items():
                cursor.execute(f'CREATE TABLE IF NOT EXISTS {table} ({schema})')

    elif database_type.lower() == 'mariadb':
        host, port, user, password, database = [i.replace(' ','') for i in open('authentication/mariadb', 'r').read().split(',')]
        connection = pymysql.connect(host=host, port=int(port), user=user, password=password, database=database, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                tables = {
                    f'{table_name}': 'ID INT AUTO_INCREMENT PRIMARY KEY, NAMA TEXT, KOORDINAT TEXT, ALAMAT TEXT, RATING FLOAT, JML_RATING INT, TAG_GOOGLE TEXT, KELURAHAN TEXT, KECAMATAN TEXT, KOTA TEXT, PROVINSI TEXT, TIPE TEXT, IDCARI INT, DATA_UPDATE DATETIME',
                    'randomized_pos': 'ID INT AUTO_INCREMENT PRIMARY KEY, PROPINSI TEXT, KOTA TEXT, KECAMATAN TEXT, KELURAHAN TEXT, KODEPOS TEXT, DATA_UPDATE DATETIME'
                }
                for table, schema in tables.items():
                    cursor.execute(f'CREATE TABLE IF NOT EXISTS {table} ({schema})')
            connection.commit()
        finally:
            connection.close()

    else:
        print('Database tidak dikenal')

# cek database, kalau kosong isi randomized
def random_pos_check(database_type):
    df_cari = pd.read_csv('../scrape_kode_pos_indonesia/output/kode_pos.csv', dtype=str)
    df_cari = df_cari.sample(frac=1).reset_index(drop=True) # randomized order
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

    if database_type.lower() == 'sqlite':
        with sqlite3.connect('./backend/data.db') as connection:
            cursor = connection.cursor()
            cursor.execute('SELECT COUNT(*) FROM randomized_pos')
            count = cursor.fetchone()[0]
            if count == 0:
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
                    query = ('INSERT INTO randomized_pos (PROPINSI, KOTA, KECAMATAN, KELURAHAN, KODEPOS, DATA_UPDATE) VALUES (%s, %s, %s, %s, %s, %s)')                 
                    cursor.executemany(query, values)
                
            connection.commit()

        finally:
            connection.close()

    else:
        print('Database tidak dikenal')