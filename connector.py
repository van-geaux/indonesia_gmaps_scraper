from dotenv import load_dotenv

from src.logger import logger

import mysql.connector
import os
import re
import sqlite3
import yaml

def env_var_constructor(loader, node):
    value = loader.construct_scalar(node)
    pattern = re.compile(r'\$\{(\w+)\}')
    match = pattern.findall(value)
    for var in match:
        value = value.replace(f'${{{var}}}', os.getenv(var, ''))
    return value

load_dotenv()
yaml.SafeLoader.add_constructor('!env_var', env_var_constructor)

try:
    with open('config.yml', 'r') as file:
        config_content = file.read()
        config_content = re.sub(r'\$\{(\w+)\}', lambda match: os.getenv(match.group(1), ''), config_content)
        config = yaml.safe_load(config_content)
except Exception as e:
    logger.debug(f'Failed opening configuration file: {e}')

# Connect to SQLite
sqlite_conn = sqlite3.connect(config['Data_source'].get('Local').get('Location'))
cursor = sqlite_conn.cursor()
cursor.execute('SELECT * FROM randomized_address')
rows = cursor.fetchall()

# Connect to MariaDB
mariadb_conn = mysql.connector.connect(
    host=config['Data_source']['External'].get('Domain'),
    port=config['Data_source']['External'].get('Port'),
    user=config['Data_source']['External'].get('User'),
    password=config['Data_source']['External'].get('Password'),
    database=config['Data_source']['External'].get('Database_name')
)
mariadb_cursor = mariadb_conn.cursor()

# Insert data into MariaDB
for row in rows:
    mariadb_cursor.execute(
        'INSERT INTO randomized_address (ID, PROVINCE, CITY, DISTRICT, WARD, POSTAL_CODE, DATA_UPDATE) VALUES (%s, %s, %s, %s, %s, %s, %s)', row)

mariadb_conn.commit()

# Close connections
sqlite_conn.close()
mariadb_conn.close()
