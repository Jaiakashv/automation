import requests
import json
from datetime import datetime, timedelta
import time

# Headers to mimic browser
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

# Config
DAYS = 30
SLEEP_BETWEEN_CALLS = 0.5
OUTPUT_FILE = "12go_routes_results.json"

# Read routes from JSON file
with open("routes_id.json", "r", encoding="utf-8") as f:
    routes_data = json.load(f)

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

results = []

for route in routes_data:
    from_city = route["from_title"]
    to_city = route["to_title"]
    from_slug = route["from_slug"]
    to_slug = route["to_slug"]
    from_id = route["from_id"]
    to_id = route["to_id"]

    for day_offset in range(DAYS):
        date_obj = datetime.now() + timedelta(days=day_offset)
        date_str = date_obj.strftime('%Y-%m-%d')

        api_url = (
            f"https://12go.asia/api/nuxt/en/trips/search?"
    f"fromId={from_id}p&toId={to_id}p&fromSlug={from_slug}&toSlug={to_slug}"
    f"&people=1&date={date_str}&date2=undefined&csrf=&direction=forward"
        )

        print(f"Fetching: {from_city} to {to_city} on {date_str}")
        resp = requests.get(api_url, headers=BROWSER_HEADERS)
        if resp.status_code != 200:
            print(f"Failed for {from_city} to {to_city} on {date_str}")
            continue

        data = resp.json()
        operators_dict = data.get("operators", {})

        for trip in data.get("trips", []):
            params = trip.get("params", {})
            segments = trip.get("segments", [])
            travel_options = trip.get("travel_options", [])
            segment = segments[0] if segments else {}

            # Currency & Price
            currency = None
            price = None
            for option in travel_options:
                if "price" in option and "fxcode" in option["price"]:
                    currency = option["price"]["fxcode"]
                    price = option["price"].get("value")
                    break
            if not currency:
                if "price" in params and params["price"] and "fxcode" in params["price"]:
                    currency = params["price"]["fxcode"]
                    price = params["price"].get("value")
                elif "price" in segment and segment["price"] and "fxcode" in segment["price"]:
                    currency = segment["price"]["fxcode"]
                    price = segment["price"].get("value")

            # Times & Duration
            dep_time = params.get("dep_time") or segment.get("dep_time")
            arr_time = params.get("arr_time") or segment.get("arr_time")
            dep_dt = _parse_datetime_flexible(dep_time, date_str)
            arr_dt = _parse_datetime_flexible(arr_time, date_str)
            duration_str = None
            if dep_dt and arr_dt:
                if arr_dt < dep_dt:
                    arr_dt += timedelta(days=1)
                delta = arr_dt - dep_dt
                hours, minutes = divmod(int(delta.total_seconds() // 60), 60)
                duration_str = f"{hours}h {minutes}m"

            # Operator Name
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

            results.append({
                "route_url": f"https://12go.asia/en/travel/{from_slug}/{to_slug}",
                "From": from_city,
                "To": to_city,
                "Date": date_str,
                "Departure Time": dep_time,
                "Arrival Time": arr_time,
                "Transport Type": params.get("vehclasses", [None])[0],
                "Duration": duration_str,
                "Price": price,
                "Operator": operator_name,
                "provider": "12go"
            })

        time.sleep(SLEEP_BETWEEN_CALLS)

# Save results
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

print(f" Done! Saved {len(results)} results to {OUTPUT_FILE}")
