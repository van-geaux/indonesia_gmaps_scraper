import os
import sys

def input_database():
    database_input = int(input('''1: Sqlite (lokal)
2: MariaDb (Online)

Pilih tipe database (ketik angka saja): '''))
    if database_input == 1:
        database = 'sqlite'
    elif database_input == 2:
        database = 'mariadb'
    else:
        print('Pilihan invalid')
    return database

def input_proxy():
    proxy_input = int(input('''1: Tanpa proxy
2: ProxyScrape
                        
Pilih tipe proxy (ketik angka saja): '''))
    if proxy_input == 1:
        proxy = ''
    elif proxy_input == 2:
        proxy = 'proxyscrape'
    else:
        print('Pilihan invalid')
    return proxy

def input_scraper():
    scraper_input = int(input('''1: Cepat (~1s per kelurahan, max 20 results per kelurahan)
2: Sedang (~6s per kelurahan, max 200 results per kelurahan)
3: Lambat (1s~3m max 200 results per kelurahan))
                              
Perbedaan sedang dan lambat:
Scraper "Sedang" baca data apa adanya dari html halaman hasil query, tidak baca javascript. Data alamat hanya alamat depan tanpa detail kecamatan kota dll, data tag hanya 1 tag utama
Scraper "Lambat" querykan lagi masing-masing data. Data alamat lengkap sesuai tampilan google maps, data tag semuanya
                              
Contoh:
Alamat lengkap "27J8+Q34, Senden, Murtigading, Kec. Sanden, Kabupaten Bantul, Daerah Istimewa Yogyakarta 55763" akan dibaca scraper "Lambat"
Tapi scraper "Sedang" hanya bisa ambil alamat utama yaitu "27J8+Q34"
                              
Kalau alamat dan tag tidak penting karena sudah ada data koordinat dan 1 tag utama, gunakan scraper "Sedang" saja
                          
Pilih jenis scraper (ketik angka saja): '''))
    return scraper_input

def get_scraper_data():
    lanjut_input = input('Apakah ingin melanjutkan query sebelumnya? (Y/N): ')

    if lanjut_input.lower() in ['n', 'no']:
        database = input_database()
        print('\n')
        proxy = input_proxy()  
        print('\n')  

        jenis = input('Masukkan query pencarian (tidak boleh kosong) (contoh: "company registry" atau "restaurant"): ')
        print('\n')

        if not jenis.strip():
            print("Input invalid")

        propinsi = input('''Masukkan provinsi pencarian, pastikan sama persis dengan nama di database kodepos
(boleh dikosongkan) (contoh: "JAWA TENGAH"): ''')
        print('\n')
        kota = input('''Masukkan kota pencarian, pastikan sama persis dengan nama di database kodepos
(boleh dikosongkan) (contoh: "MAGELANG"): ''')
        print('\n')
        kecamatan = input('''Masukkan kecamatan pencarian, pastikan sama persis dengan nama di database kodepos
(boleh dikosongkan) (contoh: "LARANGAN"): ''')
        print('\n')
        kelurahan = input('''Masukkan kelurahan pencarian, pastikan sama persis dengan nama di database kodepos
(boleh dikosongkan) (contoh: "SITANGGAL"): ''')

        scraper_input = input_scraper()

        input_last_query = f'{database},{proxy},{jenis},{propinsi},{kota},{kecamatan},{kelurahan},{scraper_input}'

        if os.path.exists('backend/last_query'):
            os.remove('backend/last_query')

        with open('backend/last_query', 'w') as file:
            file.write(input_last_query)

    elif lanjut_input.lower() in ['y', 'yes']:
        database, proxy, jenis, propinsi, kota, kecamatan, kelurahan, scraper_input = open(f'backend/last_query', 'r').read().split(',')
        scraper_input = int(scraper_input)
        last_confirmation = input(f'''Melanjutkan query dengan konfigurasi berikut:
Database: {database}
Proxy: {proxy}
Pencarian: {jenis}
Provinsi: {propinsi}
Kecamatan: {kecamatan}
Kelurahan: {kelurahan}
Scraper: {'Cepat' if scraper_input == 1 else 'Sedang' if scraper_input == 2 else 'Lambat'}

Yakin akan melanjutkan dengan konfigurasi itu? (Y/N):''')
        
        if last_confirmation in ['y', 'yes']:
            pass
        elif last_confirmation in ['n', 'no']:
            sys.exit()
        else:
            print('Input invalid')

    else:
        print('Input invalid')

    return database, proxy, jenis, propinsi, kota, kecamatan, kelurahan, scraper_input