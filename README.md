# Indonesia Google Map Business Scraper
My first just-for-fun project after learning python

## How to use
### Easy way
- Make sure to have python installed
- Just run main.exe

### Manual way
- Make sure to have python installed
- Create an environment
```
python -m venv env
```
- Activate the environment
```
env\Scripts\activate
```
- Install the dependencies
```
pip install -r requirements.txt
```
- Run main.py
```
python main.py
```

## Database Setup
### SQLite
- Nothing to setup, just run the program and choose SQLite

### MariaDb
- Create a database first. i.e. 'scrape_map'
- Create an 'authentication' directory
- Inside it create a file 'mariadb' with no extension
- Fill it with 'host,port,user,password,dbname'
Example: mariadb.tld.com,3306,mariauser,mariapassword,scrape_map

## Proxy Setup
- I use proxyscrape but the setup should be the same for whatever proxy
- Create an 'authentication' directory
- Inside it create a file 'proxyscrape' with no extension, the name is still 'proxyscrape' even if you use other proxy
- Fill it with 'user,password,host:port'
Example: proxyuser,proxypassword,proxy.com:6060

## Features
- Scrape based on business type and region
- Scrape entirety of Indonesian regions
- Pause/exit mid scrape and continue from last scrape
- 3 scrapers-depth options
- Can use local database
- Can use proxy

## 3 Scrapers Options
- Fast, only read html from query result with no other action
- Medium, utilize selenium to scroll results to get maximum  amount of businesses from each queries
- Slow, same as medium but also open each businesses to get more accurate data

## Scraped Data
- Name
- Coordinate
- Address
- Rating value
- Rating amount
- Google tag(s)

Can expand to other data easily
