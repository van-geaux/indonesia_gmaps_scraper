from dotenv import load_dotenv

from src.backend import db_check
from src.input import input_worker
from src.logger import *

import os
import psutil
import re
import sys
import signal
import yaml

def terminate_subprocesses():
    current_process = psutil.Process()
    for child in current_process.children(recursive=True):
        child.terminate()
    _, still_alive = psutil.wait_procs(current_process.children(recursive=True), timeout=5)
    for child in still_alive:
        child.kill()

def signal_handler(sig, frame):
    print("\nSignal received, terminating subprocesses...")
    terminate_subprocesses()
    sys.exit(0)

def env_var_constructor(loader, node):
    value = loader.construct_scalar(node)
    pattern = re.compile(r'\$\{(\w+)\}')
    match = pattern.findall(value)
    for var in match:
        value = value.replace(f'${{{var}}}', os.getenv(var, ''))
    return value

def main():
    load_dotenv()
    yaml.SafeLoader.add_constructor('!env_var', env_var_constructor)

    try:
        with open('config.yml', 'r') as file:
            config_content = file.read()
            config_content = re.sub(r'\$\{(\w+)\}', lambda match: os.getenv(match.group(1), ''), config_content)
            config = yaml.safe_load(config_content)
    except Exception as e:
        logger.debug(f'Failed opening configuration file: {e}')

    try:
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        db_check(config)
        input_worker(config)

    except KeyboardInterrupt:
        logger.info('Script stopped manually.')
        pass
        
    finally:
        # print('\n')
        # input('Press Enter to exit...: ')
        logger.info('Cleaning processes...')
        terminate_subprocesses()
        sys.exit()

if __name__ == "__main__":    
    main()