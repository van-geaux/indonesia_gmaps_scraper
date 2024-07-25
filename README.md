# Indonesia Google Map Business Scraper
My first just-for-fun project after learning python
Actually support global, not just Indonesia, addresses now

## How to use
- Make sure to have python installed
- Create an environment (optional)
```
python -m venv env
```
- Activate the environment  (optional)
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

## Address Loop
- The scraper works by looping through addresses
- Create the addresses to loop through using the templates (csv or xlsx) in `backend/` (default is indonesian addresses)

## Configuration
- Edit the `config.yml` as needed
- Make sure that everything in data source is correct according to your setup
- If you want to use proxy and/or external database, rename `template.env` to `.env` and change the content accordingly

## Features
- Scrape based on business type and region
- Scrape entirety of Indonesian regions (default)
- Pause/exit mid scrape and continue from last scrape
- Can use local database
- Can use proxy
- Support global addresses

## Scraped Data
- Name
- Coordinate
- Address
- Rating value
- Rating amount
- Google tag(s)