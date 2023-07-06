import csv
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import requests
from bs4 import BeautifulSoup
from metar import Metar

from ingestion import AbstractDownloader
from ingestion.logger import logger


@dataclass
class Station:
    state_code: str
    name: str
    code4: str
    longitude: float
    latitude: float
    elevation: float
    raw: str


def _lng(lngstr: str) -> float:
    degree, minute = lngstr.split()
    abs_decimal = int(degree) + int(minute[:-1]) / 60
    if lngstr[-1] == "W":
        abs_decimal = -abs_decimal
    return abs_decimal


def _lat(latstr: str) -> float:
    degree, minute = latstr.split()
    abs_decimal = int(degree) + int(minute[:-1]) / 60
    if latstr[-1] == "S":
        abs_decimal = -abs_decimal
    return abs_decimal


def fetch_stations(
    station_file_path: os.PathLike = "./data/stations.txt",
) -> Dict[str, Station]:
    station_file_path = Path(station_file_path)
    if not station_file_path.exists():
        raise FileNotFoundError(f"Station file {station_file_path} not found")
    with station_file_path.open('r') as f:
        lines = f.readlines()
    stations = {}
    for line in lines:
        if not line.strip():
            # Empty line
            continue
        if line.startswith("!"):
            # Comment line
            continue
        if len(line) != 84:
            # State/country/header line
            continue
        code4 = line[20:24]
        stations[code4] = Station(
            state_code=line[:2],
            name=line[3:19].strip(),
            code4=code4,
            latitude=_lat(line[39:46]),
            longitude=_lng(line[47:54]),
            elevation=line[55:59].strip(),
            raw=line,
        )
    return stations


def saturation_vapor_pressure(temperature_celcius: float) -> float:
    return 611.2 * np.exp(
        17.67 * temperature_celcius / (temperature_celcius + 243.5)
    )


def relative_humidity_from_dewpoint(
    temperature_celcius: float,
    dewpoint_celcius: float,
) -> float:
    e = saturation_vapor_pressure(dewpoint_celcius)
    e_s = saturation_vapor_pressure(temperature_celcius)
    return (e / e_s)


class AwcWeatherStationDataDownloader(AbstractDownloader):

    today: str = datetime.utcnow().strftime("%Y%m%d")
    data_file_path: Path = Path(f"data/{today}.csv")

    FIELDS: List[str] = [
        'timestamp',
        'name', 'code', 'lng', 'lat', 'ele', # Station info
        'temperature_c', 'dewpoint_c', 'relativehumidity', # Temp + humidity
        'pressure_mb', 'pressuresea_mb', # Pressure
        'winddirection_deg', 'windspeed_kt', 'windgust_kt', # Wind
        'rawmetar',
    ]

    def __init__(
            self,
            stations: Dict[str, Station],
            target_dir: os.PathLike = "data",
        ):
        super().__init__()
        self.stations = stations
        self.target_dir = Path(target_dir)
        self.target_dir.mkdir(parents=True, exist_ok=True)
        self._ensure_data_file()
        self.data = self._load_data()

    def _ensure_data_file(self):
        self.today = datetime.utcnow().strftime("%Y%m%d")
        self.data_file_path = self.target_dir / f"{self.today}.csv"
        if not self.data_file_path.exists():
            self.data_file_path.touch()

    def _load_data(self) -> List[Dict[str, str]]:
        if not self.data_file_path.exists():
            logger.warning(
                f"Data file {self.data_file_path} not found. "
                "Will create an empty one."
            )
            self.data_file_path.exists().touch()
            return []
        with self.data_file_path.open('r') as f:
            reader = csv.DictReader(f, delimiter=',')
            return list(reader)

    def _dump_data(self):
        last_ts = None
        with self.data_file_path.open('w') as f:
            writer = csv.DictWriter(f, fieldnames=self.FIELDS, delimiter=',')
            writer.writeheader()
            for row in self.data:
                # if row['timestamp'] == last_ts:
                #     continue
                # last_ts = row['timestamp']
                writer.writerow(row)

    def download1(self, stations_to_search: List[Station]) -> bool:
        base_url = "https://www.aviationweather.gov/metar/data"
        params = {
            'ids': ",".join([s.code4 for s in stations_to_search]),
            'format': "raw",
            'hours': 0,
            'taf': "off",
            'layout': "off",
        }

        res = requests.get(base_url, params=params)
        res.raise_for_status()

        soup = BeautifulSoup(res.text, 'html.parser')
        for element in soup.find_all('code'):
            metar_raw = element.text
            logger.info(f"Fetched raw METAR: {metar_raw}")

            data = self._fetch_data_from_raw_metar(metar_raw)
            if data is not None:
                self.data.append(data)
        self._dump_data()

        return True

    def _fetch_data_from_raw_metar(
        self,
        metar_raw: str,
    ) -> Optional[Dict[str, str]]:
        metar_decoded = Metar.Metar(metar_raw, strict=False)
        station = self.stations[metar_decoded.station_id]
        if not self._metar_valid(metar_decoded):
            logger.warning(
                f"[{station.code4}] METAR data invalid "
                "with missing field around W/T/H/P."
            )
            return None

        temperature_c = metar_decoded.temp.value(units="C")
        dewpoint_c = metar_decoded.dewpt.value(units="C")
        data = {
            'timestamp': metar_decoded.time.timestamp(),
            'name': station.name,
            'code': station.code4,
            'lng': station.longitude,
            'lat': station.latitude,
            'ele': station.elevation,
            'temperature_c': temperature_c,
            'dewpoint_c': dewpoint_c,
            'relativehumidity': relative_humidity_from_dewpoint(
                temperature_c,
                dewpoint_c,
            ),
            'pressure_mb': metar_decoded.press.value(units="MB"),
            'pressuresea_mb': (
                metar_decoded.press_sea_level.value(units="MB")
                if metar_decoded.press_sea_level is not None
                else ""
            ),
            'winddirection_deg': metar_decoded.wind_dir.value(),
            'windspeed_kt': metar_decoded.wind_speed.value(units="KT"),
            'windgust_kt': (
                metar_decoded.wind_gust.value(units="KT")
                if metar_decoded.wind_gust is not None
                else ""
            ),
            'rawmetar': metar_raw,
        }
        logger.info(
            f"[{station.code4}] Fetched data: "
            f"ts={data['timestamp']}, "
            f"code={data['code']}, "
            f"temp={data['temperature_c']}°C, "
            f"pres={data['pressure_mb']}mb, "
            f"rh={round(data['relativehumidity'] * 100)}%,"
            f"wind={round(data['winddirection_deg'])}° "
            f"at {data['windspeed_kt']} knots"
        )

        return data

    def _metar_valid(self, metar: Metar.Metar) -> bool:
        return (
            metar.time is not None
            and metar.temp is not None
            and metar.dewpt is not None
            and metar.press is not None
            and metar.wind_dir is not None
            and metar.wind_speed is not None
        )
