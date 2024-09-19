from src.scraper import *
from src.printer import *

def input_worker(config):
    print('')
    worker_input = int(input('''1: Scrape google map
2: Export scrape result
3: Delete scrape result
4: Exit

Choose what you want to do (enter only number): '''))
    if worker_input == 1:
        confirmation = input(f'''\nScraping will start with these parameters:
Proxy = {config['Proxy'].get('Domain')}
Query = {config['Category']}

Search loop
  Province = {('All' if config['Address_level'].get('Province') is None else config['Address_level'].get('Province'))}
  City = {('All' if config['Address_level'].get('City') is None else config['Address_level'].get('City'))}
  District/subdistrict = {('All' if config['Address_level'].get('District/subdistrict') is None else config['Address_level'].get('District/subdistrict'))}
  Ward/village = {('All' if config['Address_level'].get('Ward/village') is None else config['Address_level'].get('Ward/village'))}

Database = {('Local SQLite' if not config['Data_source'].get('External').get('Domain') else config['Data_source'].get('External').get('Domain'))}

Confirm parameters to do/continue scraping? (Y/N): ''')
        
        if confirmation.lower() == 'y' or confirmation.lower() == 'yes':
            deep_scraper(config)
        else:
            logger.warning('[WARNING] Input not recognized.')

    elif worker_input == 2:
        data_print(config)

    elif worker_input == 3:
        data_delete(config)
        pass

    elif worker_input == 4:
        pass

    else:
        logger.warning('[WARNING] Invalid input')