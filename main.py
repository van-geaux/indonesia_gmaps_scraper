from src.backend import db_check, random_pos_check
from src.scraper import get_driver, map_scraper, map_scraper_with_scrolls
from src.utilities import clean_table_name

# TODO loop keseluruhan kode berdasarkan database jenis

def main():
    try:
        print("Running script1. Press Ctrl+C to interrupt.")
        while True:
            jenis = 'company registry'
            filter_wilayah = ['PROPINSI = "JAWA TENGAH"',]

            jenis_table = clean_table_name(jenis, filter_wilayah)

            db_check(jenis_table)
            random_pos_check()

            driver = get_driver() # driver pertama di luar function agar bisa close driver kalau manual interrupt

            # PILIH SALAH SATU
            # map_scraper(jenis, jenis_table, df_cari)
            map_scraper_with_scrolls(jenis, jenis_table, filter_wilayah, driver)

    except KeyboardInterrupt:
        print("\nScript1 interrupted. Running cleanup script.")
        driver.close()

if __name__ == "__main__":
    main()