# 12Go Scraper & Automation  

This project contains a web scraper and automation scripts for fetching route data from 12Go and optionally importing it into a database.  

## 📂 Files Overview  

- **`12go_scrapper.py`**  
  Run this file to scrape data.  
  - Requires `routes_id.json` (contains IDs for popular routes).  
  - Output: Data will be saved in **JSON format**.  

- **`12go.py`**  
  Automation script that automatically imports the scraped data into your database.  

## ⚙️ Setup  

1. Clone this repository:  
   ```bash
   git clone <your-repo-url>
   cd <your-repo-name>
2. install dependencies 
  ```bash 
  pip install -r requirements.txt
3.routes_id.json sample :
[
  {
    "title": "Bangkok → Chiang Mai",
    "from_title": "Bangkok",
    "to_title": "Chiang Mai",
    "from_slug": "bangkok",
    "to_slug": "chiang-mai",
    "from_id": "1",
    "to_id": "44",
    "search_api": "https://12go.asia/api/nuxt/en/trips/search?fromId=1p&toId=44p&fromSlug=bangkok&toSlug=chiang-mai&people=2&date=2025-08-17&date2=undefined&csrf=dbSGug&direction=forward&cartHash=&outboundTripKey=&outboundGodate="
  }
]
4.to run 12go_scrapper.py 
  ```bash 
  12go_scrapper.py

5. to run  12go.py
  use this in github actions its automated add this code and  yaml file in your github and create an github secrets
  database connection must be imported in the github secrets and variable -> actions -> new repository and then paste your connection string , the variable must be DATABASE_URL = your connection string