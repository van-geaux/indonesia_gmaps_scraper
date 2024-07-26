import logging
import os
import re
import yaml

with open('config.yml', 'r') as file:
    config_content = file.read()
    config = yaml.safe_load(config_content)
try:
    log_level_str = config.get('Log_level').upper()
except:
    log_level_str = 'other'

log_level_console = getattr(logging, log_level_str, logging.INFO)
log_level_file = getattr(logging, log_level_str, logging.WARNING)

# Create a custom logger
logger = logging.getLogger(__name__)

# Set the logger level
logger.setLevel(logging.DEBUG)

# Create handlers: one for the terminal (console) and one for the file
console_handler = logging.StreamHandler()
file_handler = logging.FileHandler('app.log')

# Set levels for the handlers
console_handler.setLevel(log_level_console)
file_handler.setLevel(log_level_file)

# Create formatters and add them to the handlers
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Add handlers to the logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)
