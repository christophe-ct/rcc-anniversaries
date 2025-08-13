#!/usr/bin/env python3

# ==============================================================
# A script to scrape crime-related anniversaries from Wikimedia
# sources and export them as CSV and JSON files.
#
# Data Sources:
#   • Wikipedia "On-this-day" Feed: https://api.wikimedia.org/wiki/Feed_API/Reference/On_this_day
#   • Wikidata Query Service: https://www.wikidata.org/wiki/Wikidata:SPARQL_query_service

# Crime categories used in Wikidata query:
# War crime (wd:Q475026), Murder (wd:Q132821), Assassination (wd:Q170024), Terrorism (wd:Q47293),
# Mass murder (wd:Q1683310), Hijacking (wd:Q43344), Arson (wd:Q423), Hate crime (wd:Q6498836),
# Sexual assault (wd:Q18592), Cybercrime (wd:Q1134461), Kidnapping (wd:Q151921),
# Fraud (wd:Q1192135), Organized crime (wd:Q723148)

# ==============================================================

import requests
import datetime
import pandas as pd
import re
import time
import csv
import json
import calendar
import os  # <-- CHANGE 1: Import the os module
from collections import defaultdict

# Silence pandas warnings if any arise
pd.options.mode.chained_assignment = None

# ==============================================================
# 1. Settings
# ==============================================================
# ... (this section remains the same) ...
# --- Data Source Controls ---
GET_WIKIDATA = True
GET_ON_THIS_DAY = True

# --- General Settings ---
CRIME_RE = re.compile(
    r"""
    \b(
        murder|assassin(?:ation)?|shooting|robbery|kidnap(?:ping)?|hijack|
        massacre|bomb(?:ing)?|terror(?:ism|ist)|serial\ killer|strangler|
        ripper|fraud|embezzl(?:e|ement)|money\ laundering|insider\ trading|
        tax\ evasion|bribery|corruption|extortion|ponzi|scam|securities\ fraud|
        pyramid\ scheme|price\ fixing|racketeering|manslaughter|assault|rape|arson
    )\b
    """,
    re.I | re.X
)
USER_AGENT = "CrimeAnnivBot/0.8 (StandaloneScript)"
YEAR_CUTOFF = 1900
MAX_PAGES = 5
PAUSE_SEC = 1.0  # Pause between API calls to be polite

# ==============================================================
# 2. Wikidata Query Functions
# ==============================================================
def query_wikidata_crimes(year_cutoff):
    """Queries the Wikidata SPARQL endpoint for criminal incidents."""
    print("--- Wikidata: Preparing SPARQL query... ---")
    sparql_query = f"""
    SELECT ?item ?itemLabel ?date ?crimeTypeLabel ?article ?countryLabel
    WHERE {{
      VALUES ?crimeType {{
        wd:Q475026 wd:Q132821 wd:Q170024 wd:Q47293 wd:Q1683310 wd:Q43344
        wd:Q423 wd:Q6498836 wd:Q18592 wd:Q1134461 wd:Q151921 wd:Q1192135 wd:Q723148
      }}
      ?item wdt:P31 ?crimeType.
      ?item wdt:P585|wdt:P580 ?date.

      OPTIONAL {{ ?item wdt:P17 ?country. }} # Get the country

      OPTIONAL {{
        ?article schema:about ?item;
                 schema:inLanguage "en";
                 schema:isPartOf <https://en.wikipedia.org/>.
      }}
      FILTER(YEAR(?date) >= {year_cutoff})
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en", "en-gb". }}
    }}
    """
    endpoint_url = "https://query.wikidata.org/sparql"
    headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
    params = {"query": sparql_query}
    try:
        print("--- Wikidata: Sending query to endpoint... ---")
        response = requests.get(endpoint_url, headers=headers, params=params, timeout=90)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"--- Wikidata: Query failed! Error: {e} ---")
        return None


def process_wikidata_results(data, rows, events_by_date):
    """Processes JSON from Wikidata into our standard format."""
    if not data or 'results' not in data or 'bindings' not in data['results']:
        print("--- Wikidata: No results found or data is malformed. ---")
        return
    results = data['results']['bindings']
    print(f"--- Wikidata: Received {len(results)} raw results. Processing now... ---")
    processed_count, skipped_count = 0, 0
    for item in results:
        try:
            date_str = item.get('date', {}).get('value')
            if not date_str:
                raise ValueError("Missing date")
            dt = datetime.datetime.fromisoformat(date_str.replace('Z', '+00:00'))

            event_text = item.get('itemLabel', {}).get('value')
            if not event_text or event_text.startswith('Q'):
                raise ValueError("Missing or invalid itemLabel")

            country = item.get('countryLabel', {}).get('value', '')
            crime_type = item.get('crimeTypeLabel', {}).get('value', 'crime').lower()
            url = item.get('article', {}).get('value', '')
            mm, dd, event_year = f"{dt.month:02}", f"{dt.day:02}", dt.year
            date_label = f"{calendar.month_name[dt.month]} {dt.day}"

            row = {
                "source": "Wikidata", "month": mm, "day": dd, "year_of_event": event_year,
                "event": event_text, "title": event_text, "description": "Source: Wikidata",
                "crime_type": crime_type, "country": country, "url": url,
                "latitude": "", "longitude": "", "extract": "", "related_topics": "",
                "topic_page_urls": ""
            }
            rows.append(row)

            payload = {k: v for k, v in row.items() if k not in ['month', 'day', 'latitude', 'longitude', 'source']}
            events_by_date[date_label].append({event_text: payload})
            processed_count += 1
        except (ValueError, KeyError):
            skipped_count += 1
            continue
    print(f"\n--- Wikidata: Processing complete. Processed: {processed_count}, Skipped: {skipped_count} ---\n")

# ==============================================================
# 3. Main Execution
# ==============================================================
def main():
    """Main function to run the scraper and export files."""
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    all_rows = []
    events_by_date = defaultdict(list)

    # --- Fetch from Wikidata ---
    if GET_WIKIDATA:
        wikidata_json = query_wikidata_crimes(YEAR_CUTOFF)
        if wikidata_json:
            process_wikidata_results(wikidata_json, all_rows, events_by_date)

    # --- Fetch from Wikipedia 'On This Day' ---
    if GET_ON_THIS_DAY:
        print("--- On-This-Day: Starting daily feed query... ---")
        for day in range(1, 367): # Use 367 to include Dec 31st in leap years
            try:
                # This handles leap years gracefully
                dt = datetime.date(2024, 1, 1) + datetime.timedelta(days=day - 1)
            except ValueError:
                continue # Skips Feb 29 on non-leap years if we were using a fixed year
                
            mm, dd = f"{dt.month:02}", f"{dt.day:02}"
            date_label = f"{calendar.month_name[dt.month]} {dt.day}"
            if dt.day == 1:
                print(f"  Processing {calendar.month_name[dt.month]}...")
            try:
                feed_url = f"https://api.wikimedia.org/feed/v1/wikipedia/en/onthisday/events/{mm}/{dd}"
                response = session.get(feed_url, timeout=10)
                response.raise_for_status()
                data = response.json()
            except requests.exceptions.RequestException as e:
                print(f"  Could not fetch data for {date_label}: {e}")
                continue

            for ev in data.get("events", []):
                match = CRIME_RE.search(ev["text"])
                if not match:
                    continue
                event_year = ev.get("year")
                if event_year is None or event_year < YEAR_CUTOFF:
                    continue
                pages = ev.get("pages", [])
                if not pages:
                    continue
                
                crime_type = match.group(1).lower()
                first_page = pages[0]
                title = first_page["title"].replace("_", " ")
                description = first_page.get("description", "")
                url = first_page.get("content_urls", {}).get("desktop", {}).get("page", "")
                extract = first_page.get("extract", "")
                
                coordinates = ""
                for p in pages[:MAX_PAGES]:
                    if coords_data := p.get("coordinates"):
                        coordinates = coords_data
                        break
                
                related_pages = pages[1:MAX_PAGES]
                related_topics = ", ".join([p["title"].replace("_", " ") for p in related_pages])
                topic_url_list = [f'{p["title"].replace("_", " ")}: {p.get("content_urls", {}).get("desktop", {}).get("page", "")}' for p in related_pages]
                topic_page_urls = ", ".join(topic_url_list)

                row = {
                    "source": "OnThisDay", "month": mm, "day": dd, "year_of_event": event_year,
                    "event": ev["text"], "title": title, "description": description,
                    "crime_type": crime_type, "country": "", "url": url,
                    "latitude": coordinates.get("lat", "") if isinstance(coordinates, dict) else "",
                    "longitude": coordinates.get("lon", "") if isinstance(coordinates, dict) else "",
                    "extract": extract, "related_topics": related_topics, "topic_page_urls": topic_page_urls,
                }
                all_rows.append(row)

                payload = {k: v for k, v in row.items() if k not in ['month', 'day', 'latitude', 'longitude', 'source']}
                events_by_date[date_label].append({ev["text"]: payload})
            time.sleep(PAUSE_SEC)
        print("--- On-This-Day: Feed processing complete. ---")


    # --- Export results to files ---
    if all_rows:
        # v-- CHANGE 2: The entire export block below is updated --v
        print("\n--- Export: Merging and saving all data... ---")

        output_dir = "files"
        # Create the directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        column_order = [
            "source", "month", "day", "year_of_event", "country", "event", "title",
            "description", "crime_type", "url", "latitude", "longitude", "extract",
            "related_topics", "topic_page_urls"
        ]

        df = pd.DataFrame(all_rows)
        for col in column_order:
            if col not in df.columns:
                df[col] = ''
        df = df[column_order].fillna('').sort_values(["month", "day", "year_of_event"])

        timestamp = time.strftime("%Y%m%d_%H%M%S")
        base_filename = f"crime_anniversaries_merged_{timestamp}"

        # Use os.path.join to create the full file paths inside the 'files' directory
        csv_path = os.path.join(output_dir, f"{base_filename}.csv")
        json_path = os.path.join(output_dir, f"{base_filename}.json")

        df.to_csv(csv_path, index=False, encoding="utf-8", quoting=csv.QUOTE_ALL, lineterminator="\n")

        sorted_events_by_date = dict(sorted(
            events_by_date.items(),
            key=lambda item: datetime.datetime.strptime(f"{item[0]} 2024", "%B %d %Y")
        ))
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(sorted_events_by_date, f, ensure_ascii=False, indent=4)

        print(f"✅ Files saved successfully inside the '{output_dir}' folder as:\n  - {csv_path}\n  - {json_path}")
        print(f"Found {len(df)} total events.")
        # ^-- End of changed block --^
    else:
        print("\n--- Export: No data was collected. Nothing to save. ---")


if __name__ == "__main__":
    main()