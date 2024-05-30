from src.backend import db_check, random_pos_check
from src.input import get_scraper_data
from src.printer import data_print
from src.scraper import map_scraper, map_scraper_with_scrolls, map_scraper_with_scrolls_deep
from src.utilities import clean_table_name

import os
import psutil
import sys
import signal

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

def main():
    try:
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

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
        input('Tekan Enter untuk keluar...: ')

    except KeyboardInterrupt:
        print('\nScript dihentikan manual.')
        input('Tekan Enter untuk keluar...: ')
    finally:
        terminate_subprocesses()
        sys.exit()

if __name__ == "__main__":    
    main()