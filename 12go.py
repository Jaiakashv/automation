import asyncio
import aiohttp
import json
import psycopg2
from psycopg2 import extras
from datetime import datetime, timedelta
import time
import os
import sys

# --- Configuration ---
DB_CONN_STRING = os.environ.get("DATABASE_URL")
DAYS = 30
CONCURRENCY = 5
try:
    routes_data = json.load(open("routes_id.json", encoding="utf-8"))
except FileNotFoundError:
    print("Error: routes_id.json not found. Please create this file.")
    sys.exit(1)

BROWSER_HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9,ta;q=0.8,pt;q=0.7",
    "priority": "u=1, i",
    "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
}

SUPPORTED_CURRENCIES = [
    "AUD","CNY","EUR","GBP","HKD","IDR","INR","JOD","JPY",
    "LAK","LKR","MAD","MYR","NZD","PHP","SGD","THB","TRY","USD","VND"
]
HARDCODED_RATES = {
    "JOD": 123.14,
    "LAK": 0.0040,
    "LKR": 0.29,
    "MAD": 9.66,
    "VND": 0.0033
}

# --- Helper Functions ---
def get_db_connection():
    """Establishes and returns a database connection."""
    if not DB_CONN_STRING:
        print("Error: DATABASE_URL environment variable is not set.")
        return None
    try:
        return psycopg2.connect(DB_CONN_STRING)
    except psycopg2.DatabaseError as e:
        print(f"Error connecting to the database: {e}")
        return None

def get_exchange_rate_url(base_currency):
    return f"https://open.er-api.com/v6/latest/{base_currency}?symbols=INR"

def _parse_datetime_flexible(val, fallback_date_str):
    try:
        if val is None:
            return None
        if isinstance(val, (int, float)):
            return datetime.fromtimestamp(val)
        if isinstance(val, str):
            v = val.strip()
            fmts = [
                '%Y-%m-%dT%H:%M:%S%z',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d %H:%M',
            ]
            for fmt in fmts:
                try:
                    dt = datetime.strptime(v, fmt)
                    if '%z' in fmt:
                        return dt.astimezone().replace(tzinfo=None)
                    return dt
                except Exception:
                    pass
            for fmt in ('%H:%M:%S', '%H:%M'):
                try:
                    t = datetime.strptime(v, fmt)
                    base = datetime.strptime(fallback_date_str, '%Y-%m-%d')
                    return base.replace(hour=t.hour, minute=t.minute, second=getattr(t, 'second', 0))
                except Exception:
                    pass
        return None
    except Exception:
        return None

def parse_duration_minutes(v):
    if not v:
        return None
    s = str(v).lower().strip()
    total = 0
    h_match = s.find('h')
    m_match = s.find('m')
    if h_match != -1:
        try:
            total += int(s[:h_match].strip()) * 60
        except ValueError:
            pass
    if m_match != -1:
        try:
            start_pos = h_match + 1 if h_match != -1 else 0
            total += int(s[start_pos:m_match].strip())
        except ValueError:
            pass
    if total > 0:
        return total
    
    hm_match = s.split(':')
    if len(hm_match) == 2:
        try:
            return int(hm_match[0]) * 60 + int(hm_match[1])
        except ValueError:
            pass

    try:
        num = int(s)
        return num
    except ValueError:
        pass
    return None

# --- Main Scraper & Data Processor ---
async def fetch_route(session: aiohttp.ClientSession, route, date_str, sem, results, rates):
    async with sem:
        from_city = route["from_title"]
        to_city = route["to_title"]
        from_slug = route["from_slug"]
        to_slug = route["to_slug"]
        from_id = route["from_id"]
        to_id = route["to_id"]

        api_url = (
            f"https://12go.asia/api/nuxt/en/trips/search?"
            f"fromId={from_id}p&toId={to_id}p&fromSlug={from_slug}&toSlug={to_slug}"
            f"&people=1&date={date_str}&date2=undefined&csrf=&direction=forward"
        )

        try:
            async with session.get(api_url, headers=BROWSER_HEADERS) as resp:
                if resp.status != 200:
                    print(f"Failed for {from_city} -> {to_city} on {date_str} with status {resp.status}")
                    return
                data = await resp.json()
        except Exception as e:
            print(f"Error fetching {api_url}: {e}")
            return

        operators_dict = data.get("operators", {})
        for trip in data.get("trips", []):
            if not trip.get("is_visible", True) or not trip.get("bookable", True):
                continue
            params = trip.get("params", {})
            segments = trip.get("segments", [])
            travel_options = trip.get("travel_options", [])
            segment = segments[0] if segments else {}

            currency = None
            price = None
            
            for option in travel_options:
                if "price" in option and option["price"] and "fxcode" in option["price"]:
                    currency = option["price"]["fxcode"]
                    try:
                        price = float(option["price"].get("value"))
                    except (ValueError, TypeError):
                        price = None
                    if price is not None:
                        break
            if price is None and "price" in params and params["price"] and "fxcode" in params["price"]:
                currency = params["price"]["fxcode"]
                try:
                        price = float(params["price"].get("value"))
                except (ValueError, TypeError):
                        price = None
            if price is None and "price" in segment and segment["price"] and "fxcode" in segment["price"]:
                currency = segment["price"]["fxcode"]
                try:
                    price = float(segment["price"].get("value"))
                except (ValueError, TypeError):
                    price = None
            if not price or price <= 0:
                continue

            dep_time_str = params.get("dep_time") or segment.get("dep_time")
            arr_time_str = params.get("arr_time") or segment.get("arr_time")
            dep_dt = _parse_datetime_flexible(dep_time_str, date_str)
            arr_dt = _parse_datetime_flexible(arr_time_str, date_str)
            duration_str = None
            if dep_dt and arr_dt:
                if arr_dt < dep_dt:
                    arr_dt += timedelta(days=1)
                delta = arr_dt - dep_dt
                hours, remainder = divmod(int(delta.total_seconds()), 3600)
                minutes = remainder // 60
                duration_str = f"{hours}h {minutes}m"

            operator_name = (
                params.get("operator_name") or
                (travel_options[0].get("operator_name") if travel_options else None) or
                segment.get("operator_name")
            )
            if not operator_name:
                operator_id = params.get("operator") or segment.get("operator") or (
                    travel_options[0].get("operator") if travel_options else None
                )
                if operator_id and operators_dict:
                    opinfo = operators_dict.get(str(operator_id))
                    if opinfo and "name" in opinfo:
                        operator_name = opinfo["name"]

            price_inr = None
            if price is not None and price > 0:
                try:
                    if currency == "INR":
                        converted = round(price, 2)
                    elif currency in HARDCODED_RATES:
                        converted = price * HARDCODED_RATES[currency]
                    elif currency in rates and rates[currency] > 0:
                        converted = price * rates[currency]
                    else:
                        converted = None
                    
                    if converted is not None:
                        price_inr = max(0, min(round(converted, 2), 9999999.99))
                except Exception as e:
                    print(f"Error converting {currency} to INR: {e}")
                    price_inr = None

            results.append({
                "route_url": f"https://12go.asia/en/travel/{from_slug}/{to_slug}",
                "origin": from_city,
                "destination": to_city,
                "travel_date": date_str,
                "departure_time": dep_dt,
                "arrival_time": arr_dt,
                "transport_type": params.get("vehclasses", [None])[0],
                "duration_min": parse_duration_minutes(duration_str),
                "price": price,
                "currency": currency,
                "price_inr": price_inr,
                "operator_name": operator_name,
                "provider": "12go",
            })

async def main():
    start_time = time.time()
    print("Scraper started...")
    results = []
    
    sem = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        rates = {"INR": 1.0, **HARDCODED_RATES}
        currencies_to_fetch = [c for c in SUPPORTED_CURRENCIES if c != "INR" and c not in HARDCODED_RATES]
        for currency in currencies_to_fetch:
            try:
                url = get_exchange_rate_url(currency)
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get('result') == 'success' and 'rates' in data and 'INR' in data['rates']:
                            rates[currency] = data['rates']['INR']
            except Exception as e:
                rates[currency] = 1.0
        print("Exchange rates loaded:", rates)

        tasks = []
        for route in routes_data:
            for day_offset in range(DAYS):
                date_str = (datetime.now() + timedelta(days=day_offset)).strftime('%Y-%m-%d')
                tasks.append(fetch_route(session, route, date_str, sem, results, rates))
        
        print(f"Fetching data for {len(tasks)} routes over {DAYS} days...")
        await asyncio.gather(*tasks)

    # --- Database Insertion Logic ---
    if not results:
        print("❌ ERROR: The list of scraped records is empty. No data to insert.")
        return

    conn = None
    try:
        print("Connecting to the database...")
        conn = get_db_connection()
        if not conn:
            return
            
        cur = conn.cursor()

        print("Truncating the trips table to delete data and reset the ID sequence...")
        cur.execute("TRUNCATE TABLE trips RESTART IDENTITY;")
        conn.commit()
        print("Table successfully truncated and ID sequence reset. ✅")
        
        records_to_insert = [
            (
                r["route_url"],
                r["origin"],
                r["destination"],
                r["departure_time"],
                r["arrival_time"],
                r["transport_type"],
                r["duration_min"] or 0,
                r["price"] or 0,
                r["price_inr"] or 0,
                r["currency"],
                r["travel_date"],
                r["operator_name"],
                r["provider"]
            )
            for r in results
        ]
        
        print(f"Importing {len(records_to_insert)} new records...")
        
        chunk_size = 1000
        for i in range(0, len(records_to_insert), chunk_size):
            chunk = records_to_insert[i:i + chunk_size]
            extras.execute_values(cur, """
            INSERT INTO trips (
                route_url, origin, destination,
                departure_time, arrival_time, transport_type,
                duration_min, price, price_inr, currency,
                travel_date, operator_name, provider
            ) VALUES %s
            """, chunk, page_size=chunk_size)
            conn.commit()
            print(f"Processed {i + len(chunk)} records...")
        
        end_time = time.time()
        duration = end_time - start_time
        print(f"✅ Import completed! Imported {len(records_to_insert)} records in {duration:.2f} seconds.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error during database operation: {error}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Database connection closed. 👋")

if __name__ == "__main__":
    asyncio.run(main())