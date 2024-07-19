from src.scraper import *
from src.printer import *

def input_worker(config):
    worker_input = int(input('''1: Scrape google map
2: Export scrape result
3: Delete scrape result
4: Create new address table
5: Exit

Choose what you want to do (enter only number): '''))
    if worker_input == 1:
        confirmation = input(f'''\nScraping will start with these parameters:
Proxy = {config['Proxy'].get('Domain')}
Query = {config['Category']}

Search loop
  Province = {config['Address_level'].get('Province')}
  City = {config['Address_level'].get('City')}
  District/subdistrict = {config['Address_level'].get('District/subdistrict')}
  Ward/village = {config['Address_level'].get('Ward/village')}

Database = {('Local SQLite' if config['Data_source'].get('Local').get('Location') else config['Data_source'].get('External').get('Domain'))}

Confirm parameters to do/continue scraping? (Y/N): ''')
        
        if confirmation.lower() == 'y' or confirmation.lower() == 'yes':
            deep_scraper(config)
        else:
            print('[WARNING] Input not recognized.')

    elif worker_input == 2:
        data_print(config)

    elif worker_input == 3:
        data_delete(config)
        pass

    elif worker_input == 4:
        print('[WARNING] Create new adress table not implemented yet. Please do so manually in the database')
        pass

    elif worker_input == 5:
        pass

    else:
        print('[WARNING] Invalid input')