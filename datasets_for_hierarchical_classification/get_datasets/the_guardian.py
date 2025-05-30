import argparse
import os
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import requests
import json

load_dotenv()

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date", type=str, required=True)
    parser.add_argument("--end_date", type=str, required=True)
    return parser

def get_the_guardian():
    args = get_args().parse_args()
    base_url = os.getenv("THE_GUARDIAN_API_URL")
    ORDER_BY = "newest"
    delta = pd.DateOffset(months=1)
    current_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")

    # Ensure current_date is the first day of its month
    current_date = current_date.replace(day=1)

    # Calculate the last day of the month
    end_of_month = current_date + pd.offsets.MonthEnd(0)  # Last day of the same month

    while current_date <= end_date:
        # First day of the month
        start_of_month = current_date.replace(day=1)
        # Last day of the month
        last_day_of_month = start_of_month + pd.offsets.MonthEnd(0)

        start_of_month_str = current_date.strftime('%Y-%m-%d')
        last_day_of_month_str = last_day_of_month.strftime('%Y-%m-%d')

        print(f"Fetching data from {start_of_month.strftime('%Y-%m-%d')} to {last_day_of_month.strftime('%Y-%m-%d')}")

        # Move to the next month
        current_date = current_date + pd.DateOffset(months=1)
        API_URL = (
                f"{base_url}&from-date={start_of_month_str}&to-date={last_day_of_month_str}"
                + f"&order-by={ORDER_BY}&page-size=50&show-fields=trailText%2Cheadline"
                + "&show-tags=keyword"
        )
        response = requests.get(API_URL)
        final_results = []
        print(API_URL)
        print(response.status_code)
        if response.status_code == 200:
            number_of_pages = response.json()["response"]["pages"]
            for page in range(number_of_pages):
                url = (
                        f"{base_url}&from-date={start_of_month_str}&to-date={last_day_of_month_str}"
                        + f"&order-by={ORDER_BY}&page-size=50&page={str(page)}&"
                        + "show-fields=trailText%2Cheadline&show-tags=keyword"
                )

                page_response = requests.get(url)
                if page_response.status_code == 200:
                    results = page_response.json()["response"]["results"]
                    final_results = final_results + results
        if len(final_results) > 0:
            json_string = json.dumps({"results": final_results})
            exist = os.path.exists(
                os.getenv("DATASETS_FOLDER_THE_GUARDIAN_ORIGINAL_JSON")
            )
            if not exist:
                os.makedirs(os.getenv("DATASETS_FOLDER_THE_GUARDIAN_ORIGINAL_JSON"))
            with open(
                    os.getenv("DATASETS_FOLDER_THE_GUARDIAN_ORIGINAL_JSON")
                    + "/the_guardian_"
                    + str(start_of_month.strftime("%Y-%m-%d"))
                    + str(last_day_of_month.strftime("%Y-%m-%d"))
                    + ".json",
                    "w",
            ) as outfile:
                outfile.write(json_string)
        current_date = last_day_of_month + timedelta(days=1)

if __name__ == "__main__":
    get_the_guardian()