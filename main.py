import argparse
import asyncio
import datetime as dt
import os

import pytz

from data_fetcher import DataFetcher

urls = {
    "max_temp": "https://meteo.hr/podaci.php?section=podaci_vrijeme&param=dnevext",
    "min_temp": "https://meteo.hr/podaci.php?section=podaci_vrijeme&param=dnevext&el=tn",
    "min_temp_5cm": "https://meteo.hr/podaci.php?section=podaci_vrijeme&param=dnevext&el=t5",
    "oborine": "https://meteo.hr/podaci.php?section=podaci_vrijeme&param=oborina",
    "snijeg": "https://meteo.hr/podaci.php?section=podaci_vrijeme&param=snijeg_n"
}

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("--file_types", dest="file_types", nargs="+", choices=["csv", "json", "xlsx", "html"],
                        required=True)
arg_parser.add_argument("--folder", dest="folder", type=str, default="files")
args = arg_parser.parse_args()
tz = pytz.timezone("Europe/Zagreb")


async def main():
    today = dt.datetime.now(tz).date()
    for k, v in urls.items():
        data_fetcher = DataFetcher(v)
        try:
            await data_fetcher.parse("table-aktualni-podaci")
            base_path = os.path.join(args.folder, today.strftime('%Y/%m'))
            file_name = os.path.join(base_path, f"{k}_{today}")
            os.makedirs(base_path, exist_ok=True)
            for x in args.file_types:
                data_fetcher.save_to_file(x, file_name)
        except Exception as e:
            print(f"There was a problem with {k}, error: {e}")
            continue


asyncio.run(main())

