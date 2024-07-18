import os
import pandas as pd
import sqlite3
import tabula 
import urllib.request

from datetime import datetime

def directory_check():
    if not os.path.exists('backend/'):
        os.makedirs('backend/')

def kodepos():
    tarik_kodepos = input('Peringatan, script ini akan melakukan penarikan ulang data kodepos dari kodepos.posindonesia.co.id. Lanjutkan proses? (Y/N): ')
    
    if tarik_kodepos in ['N', 'n']:
        print('Tarik ulang kode pos dibatalkan')
    elif tarik_kodepos in ['Y', 'y']:
        directory_check()

        if os.path.exists('backend/kodepos.pdf'):
            os.remove('backend/kodepos.pdf')

        urllib.request.urlretrieve('https://kodepos.posindonesia.co.id/CariKodepos/download', 'backend/kodepos.pdf')

        print('Penarikan data dimulai, mohon tunggu...')
        dfs = tabula.read_pdf('backend/kodepos.pdf', pages='all')

        with sqlite3.connect(f"./backend/data.db") as connection:
            cursor = connection.cursor()

            cursor.execute('DROP TABLE IF EXISTS kode_pos')

        with sqlite3.connect(f"./backend/data.db") as connection:
            cursor = connection.cursor()
            tables = {
                    "kode_pos": '"ID" INTEGER PRIMARY KEY NOT NULL, "PROPINSI" TEXT, "KOTA" TEXT, "KECAMATAN" TEXT, "KELURAHAN" TEXT, "KODEPOS" INTEGER, DATA_TIME DATETIME',
                }
            for table, schema in tables.items():
                cursor.execute(f'CREATE TABLE IF NOT EXISTS {table} ({schema})')

        with sqlite3.connect(f"./backend/data.db") as connection:
            cursor = connection.cursor()
            query = 'INSERT INTO kode_pos (PROPINSI, KOTA, KECAMATAN, KELURAHAN, KODEPOS, DATA_TIME) VALUES (?, ?, ?, ?, ?, ?)'

            for i in range(len(dfs[0])):
                provinsi = dfs[0].iloc[i].iloc[0]
                kabupaten = dfs[0].iloc[i].iloc[1]
                kecamatan = dfs[0].iloc[i].iloc[2]
                desa = dfs[0].iloc[i].iloc[3]
                kodepos = int(dfs[0].iloc[i].iloc[4])

                params = (provinsi, kabupaten, kecamatan, desa, kodepos, datetime.now())
                cursor.execute(query, params)

            print('Halaman 1 selesai')

            for i in range(1,len(dfs)):
                new_row = pd.DataFrame([dfs[i].columns], columns=dfs[i].columns)
                # new_row.rename(columns={new_row.columns[0]: 'NM_PROVINSI',
                #                         new_row.columns[1]: 'NM_KABUPATEN',
                #                         new_row.columns[2]: 'NM_KECAMATAN',
                #                         new_row.columns[3]: 'NM_DESA',
                #                         new_row.columns[4]: 'KODEPOS'}, inplace=True)
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

        print('\nSeluruh kode pos selesai diinput')
    
    else:
        print('Input invalid')