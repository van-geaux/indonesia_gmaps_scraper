import pandas as pd
import pymysql
import sqlite3

def data_print():
    database_options = int(input('''1: Sqlite (lokal)
2: MariaDb (Online)

Pilih tipe database (ketik angka saja): '''))
    
    if database_options == 1:
        with sqlite3.connect('backend/data.db') as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
        count = 0
        table_list = ''
        for table in tables:
            count += 1
            table_list += f'{count}. {table}\n'

        table_option = int(input(f'{table_list}\nPilih table yang ingin dicetak (ketik angka saja): '))

        try:
            with sqlite3.connect('backend/data.db') as connection:
                cursor = connection.cursor()
                query = f'SELECT * FROM {tables[table_option - 1][0]}'
                data = pd.DataFrame(pd.read_sql_query(query, connection))
                data.to_csv(f'output/{tables[table_option - 1][0]}.csv')

            print(f'Tabel telah disimpan di "output/{tables[table_option - 1][0]}.csv"')
        except Exception as e:
            print(e)
            print('Print table gagal')

    elif database_options == 2:
        host, port, user, password, database = [i.replace(' ','') for i in open('authentication/mariadb', 'r').read().split(',')]
        connection = pymysql.connect(host=host, port=int(port), user=user, password=password, database=database, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        
        try:
            with connection.cursor() as cursor:
                cursor = connection.cursor()
                cursor.execute("SHOW TABLES;")
                tables = cursor.fetchall()
        except Exception as e:
            print(e)
            print('Print table gagal')
        finally:
            connection.close()

        count = 0
        table_list = ''
        for table in tables:
            count += 1
            table_list += f'{count}. {table}\n'

        table_option = int(input(f'{table_list}\nPilih table yang ingin dicetak (ketik angka saja): '))

        try:
            with connection.cursor() as cursor:
                cursor = connection.cursor()
                query = f'SELECT * FROM {tables[table_option - 1][0]}'
                data = pd.DataFrame(pd.read_sql_query(query, connection))
                data.to_csv(f'output/{tables[table_option - 1][0]}.csv')

                print(f'Tabel telah disimpan di "output/{tables[table_option - 1][0]}.csv"')
        except Exception as e:
            print(e)
            print('Print table gagal')
        finally:
            connection.close()

    else:
        print('Pilihan invalid')