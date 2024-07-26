from src.input import *
from src.logger import *

import pandas as pd
import pymysql
import sqlite3

def data_print(config):
    if config['Data_source']['Local'].get('Location'):
        logger.debug('Reading sqlite tables')
        with sqlite3.connect(config['Data_source'].get('Local').get('Location')) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
        count = 0
        table_list = ''
        for table in tables:
            count += 1
            table_list += f'{count}. {table}\n'

        table_option = int(input(f'{table_list}\nChoose the table you want to export (enter only number): '))

        try:
            logger.debug('Getting table data')
            with sqlite3.connect(config['Data_source']['Local'].get('Location')) as connection:
                cursor = connection.cursor()
                query = f'SELECT * FROM {tables[table_option - 1][0]}'
                data = pd.DataFrame(pd.read_sql_query(query, connection))
                data.to_csv(f'output/{tables[table_option - 1][0]}.csv')

            print(f'Table saved in "output/{tables[table_option - 1][0]}.csv"')
        except Exception as e:
            print(f'[ERROR] Failed with {e}')

    elif config['Data_source']['External'].get('Type'):
        logger.debug('Getting external database configuration')
        host = config['Data_source']['External'].get('Domain')
        port = config['Data_source']['External'].get('Port')
        user = config['Data_source']['External'].get('User')
        password = config['Data_source']['External'].get('Password')
        database = config['Data_source']['External'].get('Database_name')
        connection = pymysql.connect(host=host, port=int(port), user=user, password=password, database=database, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        
        logger.debug('Connecting to external database')
        try:
            with connection.cursor() as cursor:
                cursor = connection.cursor()
                cursor.execute("SHOW TABLES;")
                tables = cursor.fetchall()
        except Exception as e:
            print(f'[ERROR] Export failed with {e}')
        finally:
            connection.close()

        count = 0
        table_list = ''
        for table in tables:
            count += 1
            table_list += f'{count}. {table}\n'

        table_option = int(input(f'{table_list}\nChoose the table you want to export (enter only number): '))

        try:
            logger.debug('Getting table data')
            with connection.cursor() as cursor:
                cursor = connection.cursor()
                query = f'SELECT * FROM {tables[table_option - 1][0]}'
                data = pd.DataFrame(pd.read_sql_query(query, connection))
                data.to_csv(f'output/{tables[table_option - 1][0]}.csv')

                print(f'[SUCCESS] Table saved in "output/{tables[table_option - 1][0]}.csv"')
        except Exception as e:
            print(e)
            print(f'[ERROR] Export failed with {e}')
        finally:
            connection.close()

    else:
        print('[WARNING] Invalid input')

def data_delete(config):
    if config['Data_source']['Local'].get('Location'):
        logger.debug('Reading sqlite tables')
        with sqlite3.connect(config['Data_source'].get('Local').get('Location')) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
        count = 0
        table_list = ''
        for table in tables:
            count += 1
            table_list += f'{count}. {table}\n'

        table_option = int(input(f'{table_list}\nChoose the table you want to delete (enter only number): '))
        final_confirm = input(f'Do you really want to delete that table? The process is irreversible (Y/N): ')
        
        if final_confirm.lower() == 'y':
            logger.debug('Deleting table from sqlite')
            try:
                with sqlite3.connect('backend/data.db') as connection:
                    cursor = connection.cursor()
                    query = f'DROP TABLE {tables[table_option - 1][0]}'
                    cursor.execute(query)

                logger.info(f'Table {tables[table_option - 1][0]} succesfully deleted')
            except Exception as e:
                logger.error(f'Delete failed with {e}')

        elif final_confirm.lower() == 'n':
            pass
        else:
            logger.warning('Invalid input')

    elif config['Data_source']['External'].get('Type'):
        logger.debug('Reading external database configuration')
        host = config['Data_source']['External'].get('Domain')
        port = config['Data_source']['External'].get('Port')
        user = config['Data_source']['External'].get('User')
        password = config['Data_source']['External'].get('Password')
        database = config['Data_source']['External'].get('Database_name')
        connection = pymysql.connect(host=host, port=int(port), user=user, password=password, database=database, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        
        try:
            logger.debug('Connecting to external database')
            with connection.cursor() as cursor:
                cursor = connection.cursor()
                cursor.execute("SHOW TABLES;")
                tables = cursor.fetchall()
        except Exception as e:
            logger.error(f'Failed with {e}')
        finally:
            connection.close()

        count = 0
        table_list = ''
        for table in tables:
            count += 1
            table_list += f'{count}. {table}\n'

        table_option = int(input(f'{table_list}\nChoose the table you want to delete (enter only number): '))
        final_confirm = input(f'Do you really want to delete that table? The process is irreversible (Y/N): ')

        if final_confirm.lower() == 'y':
            logger.debug('Deleting table from database')
            try:
                with connection.cursor() as cursor:
                    cursor = connection.cursor()
                    query = f'DROP TABLE {tables[table_option - 1][0]}'
                    cursor.execute(query)

                    logger.info(f'Table {tables[table_option - 1][0]} succesfully deleted')
            except Exception as e:
                logger.errorrint(f'Delete failed with {e}')
            finally:
                connection.close()

        elif final_confirm.lower() == 'n':
            pass
        else:
            logger.warning('Invalid input')

    else:
        logger.warning('Invalid input')