import os
import subprocess
import sys

from src.backend import db_check, random_pos_check
from src.input import get_scraper_data
from src.printer import data_print
from src.scraper import map_scraper, map_scraper_with_scrolls, map_scraper_with_scrolls_deep
from src.utilities import clean_table_name

def check_and_install_env():
    if not os.path.exists('env'):
        print('Virtual environment tidak ditemukan. Membuat env...')
        subprocess.check_call([sys.executable, '-m', 'venv', 'env'])
        
        print('Menginstall packages...')
        subprocess.check_call([os.path.join('env', 'Scripts', 'pip'), 'install', '-r', 'requirements.txt'])
    else:
        print('Virtual environment ditemukan.')

def activate_env():
    env_path = os.path.join('env', 'Scripts')
    if sys.platform == 'win32':
        env_python = os.path.join(env_path, 'python.exe')
    else:
        env_python = os.path.join(env_path, 'python')

    os.environ['VIRTUAL_ENV'] = os.path.abspath('env')
    os.environ['PATH'] = env_path + os.pathsep + os.environ['PATH']
    sys.executable = env_python
    sys.path = [env_path] + sys.path

def main():
    check_and_install_env()
    activate_env()
    print('\n')

    print("Menjalankan script. Tekan Ctrl+C untuk membatalkan.")
    print('\n')

    start_options = int(input('''1. Map scraping
2. Simpan data hasil scraping
                          
Pilih kegiatan (ketik angka saja): '''))
    
    if start_options == 1:
        database, proxy, jenis, propinsi, kota, kecamatan, kelurahan, scraper_input = get_scraper_data()

        print('\n')
        filter_wilayah = {
            'PROPINSI': propinsi.upper(),
            'KOTA': kota.upper(),
            'KECAMATAN': kecamatan.upper(),
            'KELURAHAN': kelurahan.upper()
        }

        db_check(database, clean_table_name(jenis, filter_wilayah))
        random_pos_check(database)

        if scraper_input == 1:
            map_scraper(database, jenis, filter_wilayah, proxy)
        elif scraper_input == 2:
            map_scraper_with_scrolls(database, jenis, filter_wilayah, proxy)
        elif scraper_input == 3:
            map_scraper_with_scrolls_deep(database, jenis, filter_wilayah, proxy)
        else:
            print('Pilihan invalid')

    elif start_options == 2:
        data_print()

    else:
        print('Pilihan invalid')

    print('\n')
    input('Tekan Enter untuk keluar...')

if __name__ == "__main__":    
    main()