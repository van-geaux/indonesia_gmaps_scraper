from datetime import datetime

import csv
import pandas as pd
import pymysql
import sqlite3
import tabula

from src.logger import logger

# logging.basicConfig(filename='error.log', level=logging.ERROR)

def clean_table_name(category, address_filter=''):
    logger.debug('Cleaning table name')
    try:
        province = address_filter.get('Province')
        city = address_filter.get('City')
        district = address_filter.get('District/subdistrict')
        ward = address_filter.get('Ward/village')

        table_name = category.replace(' ', '')

        if province:
            table_name += f'_{province.replace(' ','').lower()}'
        if city:
            table_name += f'_{city.replace(' ','').lower()}'
        if district:
            table_name += f'_{district.replace(' ','').lower()}'
        if ward:
            table_name += f'_{ward.replace(' ','').lower()}'
    except Exception as e:
        logger.error(f'Cleaning table name failed: {e}')
        
    return table_name

def copy_table(table_name, source_db, dest_db):
    logger.debug('Copying table from sqlite')
    try:
        src_conn = sqlite3.connect(source_db)
        src_cursor = src_conn.cursor()
        
        dest_conn = sqlite3.connect(dest_db)
        dest_cursor = dest_conn.cursor()
        
        src_cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        create_table_sql = src_cursor.fetchone()[0]
        
        dest_cursor.execute(create_table_sql)
        
        src_cursor.execute(f"SELECT * FROM {table_name};")
        rows = src_cursor.fetchall()
        
        src_cursor.execute(f"PRAGMA table_info({table_name});")
        columns = [info[1] for info in src_cursor.fetchall()]
        columns_str = ', '.join(columns)
        
        for row in rows:
            placeholders = ', '.join(['?'] * len(row))
            dest_cursor.execute(f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders});", row)
        
        dest_conn.commit()
        src_conn.close()
        dest_conn.close()
    except Exception as e:
        logger.error(f'Copying table from sqlite failed: {e}')

def table_check(db_path, table_name):
    logger.debug('Checking if database table exists')
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
        table_exists = cursor.fetchone() is not None
        
        conn.close()
    except Exception as e:
        logger.error(f'Checking if database table exists failed: {e}')
    
    return table_exists

def db_check(config):
    try:
        if '.db' in config['Data_source']['Local'].get('Location'):
            try:
                if table_check(config['Data_source']['Local'].get('Location'), config['Data_source']['Local'].get('Address_table_name')):
                    pass
                else:
                    logger.info(f"Addresses database in {config['Data_source']['Local'].get('Location')} is empty, copying from template...")
                    if config['Data_source']['Local'].get('Csv_location'):
                        try:
                            logger.debug(f'Preparing csv file')
                            with open(config['Data_source']['Local'].get('Csv_location'), 'r') as csvfile:
                                logger.info(f'Copying from {config['Data_source']['Local'].get('Csv_location')}')
                                csvreader = csv.reader(csvfile)
                                headers = next(csvreader)
                                headers = [header for header in headers if header]
                                if 'DATA_UPDATE' in headers:
                                    headers.remove('DATA_UPDATE')
                                columns = ', '.join([f'{header} TEXT' for header in headers[1:]])
                                create_table_query = f"""
                                CREATE TABLE IF NOT EXISTS {config['Data_source']['Local'].get('Address_table_name')} (
                                    ID INTEGER PRIMARY KEY NOT NULL, 
                                    {columns}, 
                                    DATA_UPDATE DATETIME
                                );
                                """
                                insert_query = f"""
                                INSERT INTO {config['Data_source']['Local'].get('Address_table_name')} ({', '.join(headers[1:])}) 
                                VALUES ({', '.join(['?' for _ in headers[1:]])});
                                """
                                logger.debug(f'Writing csv to database failed')
                                try:
                                    with sqlite3.connect(config['Data_source'].get('Local').get('Location')) as connection:
                                        cursor = connection.cursor()
                                        cursor.execute(create_table_query)
                                        for row in csvreader:
                                            row = [value for header, value in zip(headers, row) if header][1:]
                                            cursor.execute(insert_query, row)
                                except Exception as e:
                                    logger.error(f'Writing csv to database failed: {e}')
                                    
                        except Exception as e:
                            logger.error(f'Preparing csv file failed: {e}')
                            raise
                    
                    elif config['Data_source']['Local'].get('Xlsx_location'):
                        logger.debug(f'Preparing xlsx file')
                        try:
                            logger.info(f'Copying from {config['Data_source']['Local'].get('Xlsx_location')}')
                            df = pd.read_excel(config['Data_source']['Local'].get('Xlsx_location'), engine='openpyxl')
                            df = df.drop(columns=['ID'])
                            df = df.drop(columns=['DATA_UPDATE'])
                            columns = ', '.join([f'{col} TEXT' for col in df.columns])
                            create_table_query = f"""
                            CREATE TABLE IF NOT EXISTS {config['Data_source']['Local'].get('Address_table_name')} (
                                ID INTEGER PRIMARY KEY NOT NULL, 
                                {columns}, 
                                DATA_UPDATE DATETIME
                            );
                            """
                            logger.debug(f'Writing xlsx to database')
                            try:
                                with sqlite3.connect(config['Data_source'].get('Local').get('Location')) as connection:
                                    cursor = connection.cursor()
                                    cursor.execute(create_table_query)
                                    for row in df.itertuples(index=False):
                                        placeholders = ', '.join(['?'] * len(df.columns))
                                        insert_query = f"INSERT INTO {config['Data_source']['Local'].get('Address_table_name')} ({', '.join(df.columns)}) VALUES ({placeholders});"
                                        cursor.execute(insert_query, row)
                            except Exception as e:
                                logger.error(f'Writing xlsx to database failed: {e}')

                        except Exception as e:
                            logger.error(f'Preparing xlsx file failed: {e}')
                            raise
                    else:
                        copy_table(config['Data_source']['Local'].get('Address_table_name'), 'backend/data_template.db', config['Data_source']['Local'].get('Location'))
                    logger.info('Table copied')

            except Exception as e:
                # logger.info('Database not detected, copying from template...')
                # shutil.copy('backend/data_template.db', config['Data_source']['Local'].get('Location'))
                # logger.info('Database copied')
                logger.error(e)
        else:
            logger.warning('No database source configured in "config.yml"')
            raise

    except Exception as e:
        logger.error(e)
        raise
    else:
        pass

def db_insert(database_type, table_name, config):
    try:
        if database_type.lower() == 'sqlite':
            logger.debug(f'Creating sqlite table for query')
            try:
                with sqlite3.connect(config['Data_source'].get('Local').get('Location')) as connection:
                    cursor = connection.cursor()
                    tables = {
                        f"{table_name}": '"ID" INTEGER PRIMARY KEY NOT NULL, "NAME" TEXT, "LONGITUDE" TEXT, "LATITUDE" TEXT, "ADDRESS" TEXT, "RATING" REAL, "RATING_COUNT" INTEGER, "GOOGLE_TAGS" TEXT, "GOOGLE_URL" TEXT, "WARD" TEXT, "DISTRICT" TEXT, "CITY" TEXT, "PROVINCE" TEXT, "TYPE" TEXT, "SEARCH_ID" INTEGER, "DATA_UPDATE" DATETIME'}
                    for table, schema in tables.items():
                        cursor.execute(f'CREATE TABLE IF NOT EXISTS {table} ({schema})')
            except Exception as e:
                logger.error(f'Creating sqlite table for query failed: {e}')

        elif database_type.lower() == 'mariadb':
            logger.debug(f'Creating mariadb table for query')
            try:
                host = config['Data_source']['External'].get('Domain')
                port = config['Data_source']['External'].get('Port')
                user = config['Data_source']['External'].get('User')
                password = config['Data_source']['External'].get('Password')
                database = config['Data_source']['External'].get('Database_name')
                connection = pymysql.connect(host=host, port=int(port), user=user, password=password, database=database, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

                try:
                    with connection.cursor() as cursor:
                        tables = {
                            f'{table_name}': 'ID INT AUTO_INCREMENT PRIMARY KEY, NAME TEXT, LONGITUDE TEXT, LATITUDE TEXT, ADDRESS TEXT, RATING FLOAT, RATING_COUNT INT, GOOGLE_TAGS TEXT, URL TEXT, WARD TEXT, DISTRICT TEXT, CITY TEXT, PROVINCE TEXT, TYPE TEXT, SEARCH_ID INT, DATA_UPDATE DATETIME'}
                        for table, schema in tables.items():
                            cursor.execute(f'CREATE TABLE IF NOT EXISTS {table} ({schema})')
                    connection.commit()
                finally:
                    connection.close()
            except Exception as e:
                logger.error(f'Creating mariadb table for query failed: {e}')

        else:
            logger.warning('Database not recognized')
    except Exception as e:
        logger.error(e)

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