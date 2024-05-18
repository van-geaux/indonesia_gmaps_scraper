from datetime import datetime

import pandas as pd
import sqlite3

def db_check(query_type):
    with sqlite3.connect("./backend/data.db") as connection:
        cursor = connection.cursor()
        tables = {
            f"{query_type}": '"ID" INTEGER PRIMARY KEY NOT NULL, "NAMA" TEXT, "KOORDINAT" TEXT, "JML_RATING" INTEGER, "ALAMAT" TEXT, "TAG_GOOGLE" TEXT, "KELURAHAN" TEXT, "KECAMATAN" TEXT, "KOTA" TEXT, "PROVINSI" TEXT, "TIPE" TEXT, "IDCARI" INTEGER, "DATA_UPDATE" DATETIME',
            "randomized_pos": '"ID" INTEGER PRIMARY KEY NOT NULL, "PROPINSI" TEXT, "KOTA" TEXT, "KECAMATAN" TEXT, "KELURAHAN" TEXT, "KODEPOS" TEXT, "DATA_UPDATE"'
        }
        for table, schema in tables.items():
            cursor.execute(f'CREATE TABLE IF NOT EXISTS {table} ({schema})')

# cek database, kalau kosong isi randomized
def random_pos_check():
    with sqlite3.connect('./backend/data.db') as connection:
        cursor = connection.cursor()
        cursor.execute('SELECT COUNT(*) FROM randomized_pos')
        count = cursor.fetchone()[0]
        if count == 0:
            # df_pos = pd.read_csv('../scrape_kode_pos_indonesia/output/kode_pos.csv')
            # df_pos = df_pos.fillna('-')
            # df_cari = pd.DataFrame(df_pos['KOTA'].unique(), columns=['KOTA'])

            df_cari = pd.read_csv('../scrape_kode_pos_indonesia/output/kode_pos.csv', dtype=str)
            df_cari = df_cari.sample(frac=1).reset_index(drop=True) # randomized order

            for i in range (0, len(df_cari)):
                provinsi = df_cari.iloc[i].iloc[0]
                kota = df_cari.iloc[i].iloc[1]
                kecamatan = df_cari.iloc[i].iloc[2]
                kelurahan = df_cari.iloc[i].iloc[3]
                kodepos = df_cari.iloc[i].iloc[4]
                update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                cursor.execute(f'INSERT INTO randomized_pos (PROPINSI, KOTA, KECAMATAN, KELURAHAN, KODEPOS, DATA_UPDATE) VALUES (?, ?, ?, ?, ?, ?)', (provinsi, kota, kecamatan, kelurahan, kodepos, update_time))

def create_new_df_cari(jenis_table, filter_wilayah=''):
    try:
        with sqlite3.connect('backend/data.db') as connection:
            cursor = connection.cursor()
            cursor.execute(f'SELECT IDCARI FROM {jenis_table} ORDER BY ID DESC LIMIT 1')
            last_cari = cursor.fetchone()[0]
    except Exception:
        last_cari = 0
        pass

    with sqlite3.connect('backend/data.db') as connection:
        query = f'SELECT PROPINSI, KOTA, KECAMATAN, KELURAHAN, KODEPOS, ID AS IDCARI FROM randomized_pos WHERE IDCARI > {last_cari}'
        if filter_wilayah:
            for i in filter_wilayah:
                query += f' AND {i}'
        df_cari = pd.DataFrame(pd.read_sql_query(query, connection))
    
    return df_cari