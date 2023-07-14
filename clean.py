import csv
import os
from datetime import datetime
from pathlib import Path

import anylearn

from pymetard import fetch_stations


if os.environ.get('ANYLEARN_TASK_ID', None) is not None:
    data_workspace = anylearn.get_dataset("yhuang/METARs").download()
else:
    data_workspace = "./tmp"


stations = fetch_stations()
s_lat_stations = {k: v for k, v in stations.items() if v.latitude < 0}

FIELDS = [
    'timestamp',
    'name', 'code', 'lng', 'lat', 'ele', # Station info
    'temperature_c', 'dewpoint_c', 'relativehumidity', # Temp + humidity
    'pressure_mb', 'pressuresea_mb', # Pressure
    'winddirection_deg', 'windspeed_kt', 'windgust_kt', # Wind
    'rawmetar',
]

root = Path(data_workspace)
for year in root.iterdir():
    if not year.is_dir():
        continue
    for month in year.iterdir():
        if not month.is_dir():
            continue
        for day in month.iterdir():
            if not day.is_file():
                continue
            try:
                datetime.strptime(day.name, "%Y%m%d.csv")
            except ValueError:
                continue
            print(f"Cleaning {str(day)}")
            with day.open('r') as f:
                reader = csv.DictReader(f, delimiter=',')
                data = list(reader)

            for i, row in enumerate(data):
                code = row['code']
                if code in s_lat_stations:
                    data[i]['lat'] = s_lat_stations[code].latitude

            with day.open('w') as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=FIELDS,
                    delimiter=',',
                )
                writer.writeheader()
                for row in data:
                    writer.writerow(row)
