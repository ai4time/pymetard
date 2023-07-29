import abc
import csv
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from metar import Metar

from pymetard.logger import logger
from pymetard.metar import (
    relative_humidity_from_dewpoint,
    Station,
)


class DateRollingCsvDownloader(abc.ABC):

    def __init__(
        self,
        target_dir: os.PathLike = "data",
    ):
        super().__init__()
        self.target_dir = Path(target_dir)
        self.target_dir.mkdir(parents=True, exist_ok=True)
        self.data = []

    def _dump_data(self):
        self.data = self._deduplicate_data(self.data)
        data_by_date = self._group_data_by_date()
        dates = sorted(data_by_date.keys())
        logger.warning(
            f"Downloader contains data from "
            f"{len(dates)} days: "
            f"{dates}"
        )
        # Merge data with existing data and dump them
        for date in dates:
            data_file_path = self._ensure_data_file(date)
            data = self._load_data_from_file(data_file_path)
            data.extend(data_by_date[date])
            data = self._deduplicate_data(data)
            self._dump_data_in_file(data, data_file_path)
        self.data = []

    @abc.abstractmethod
    def _deduplicate_data(
        self,
        data: List[Dict[str, str]],
    ) -> List[Dict[str, str]]:
        raise NotImplementedError

    @abc.abstractmethod
    def _group_data_by_date(self) -> Dict[str, List[Dict[str, str]]]:
        raise NotImplementedError

    def _ensure_data_file(
        self,
        date: Optional[str] = None,
    ) -> Path:
        if not date:
            date = datetime.utcnow().strftime("%Y%m%d")
        datetime.strptime(date, "%Y%m%d") # Will raise if invalid
        save_dir = self.target_dir / date[:4] / date[4:6]
        save_dir.mkdir(parents=True, exist_ok=True)
        data_file_path = save_dir / f"{date}.csv"
        if not data_file_path.exists():
            data_file_path.touch()
        return data_file_path

    def _load_data_from_file(
        self,
        file_path: os.PathLike,
    ) -> List[Dict[str, str]]:
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File {file_path} not found")
        with file_path.open('r') as f:
            reader = csv.DictReader(f, delimiter=',')
            return list(reader)

    def _dump_data_in_file(
        self,
        data: List[Dict[str, str]],
        file_path: os.PathLike,
    ):
        file_path = Path(file_path)
        with file_path.open('w') as f:
            writer = csv.DictWriter(
                f,
                fieldnames=self._get_csv_fields(),
                delimiter=',',
            )
            writer.writeheader()
            for row in data:
                writer.writerow(row)

    @abc.abstractmethod
    def _get_csv_fields(self) -> List[str]:
        raise NotImplementedError


class MetarCsvDownloader(DateRollingCsvDownloader):

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
        super().__init__(target_dir=target_dir)
        self.stations = stations

    def _get_csv_fields(self) -> List[str]:
        return self.FIELDS

    def _deduplicate_data(
        self,
        data: List[Dict[str, str]],
    ) -> List[Dict[str, str]]:
        dedup = {}
        for row in data:
            if row['rawmetar'] not in dedup:
                dedup[row['rawmetar']] = row
        return list(dedup.values())

    def _group_data_by_date(self) -> Dict[str, List[Dict[str, str]]]:
        data_by_date = {}
        for row in self.data:
            dt = datetime.fromtimestamp(float(row['timestamp']))
            date = dt.strftime("%Y%m%d")
            if date not in data_by_date:
                data_by_date[date] = []
            data_by_date[date].append(row)
        return data_by_date

    def _fetch_data_from_raw_metar(
        self,
        metar_raw: str,
        year: Optional[int] = None,
        month: Optional[int] = None,
    ) -> Optional[Dict[str, str]]:
        logger.debug(f"Parsing raw METAR: {metar_raw}")
        metar_raw = self._clean_raw_metar(metar_raw)
        metar_decoded = Metar.Metar(
            metar_raw,
            year=year,
            month=month,
            strict=False,
        )
        station = self.stations[metar_decoded.station_id]
        if not self._metar_valid(metar_decoded):
            logger.debug(
                f"[{station.code4}] METAR data invalid "
                "with missing field around W/T/H/P."
            )
            return None

        data = {
            'timestamp': metar_decoded.time.timestamp(),
            'name': station.name,
            'code': station.code4,
            'lng': station.longitude,
            'lat': station.latitude,
            'ele': station.elevation,
            'temperature_c': self._temperature(metar_decoded),
            'dewpoint_c': self._dewpoint(metar_decoded),
            'relativehumidity': self._relative_humidity(metar_decoded),
            'pressure_mb': self._pressure(metar_decoded),
            'pressuresea_mb': self._pressure_sea(metar_decoded),
            'winddirection_deg': self._wind_direction(metar_decoded),
            'windspeed_kt': self._wind_speed(metar_decoded),
            'windgust_kt': self._wind_gust(metar_decoded),
            'rawmetar': metar_raw,
        }
        logger.debug(
            f"[{station.code4}] Fetched data: "
            f"ts={data['timestamp']}, "
            f"code={data['code']}, "
            f"temp={data['temperature_c']}°C, " if data['temperature_c'] else ""
            f"pres={data['pressure_mb']}mb, " if data['pressure_mb'] else ""
            f"rh={round(data['relativehumidity'] * 100)}%," if data['relativehumidity'] else ""
            f"wind={round(data['winddirection_deg'])}° " if data['winddirection_deg'] else ""
            f"at {data['windspeed_kt']} knots" if data['windspeed_kt'] else ""
        )

        return data

    def _clean_raw_metar(self, metar_raw: str) -> str:
        return metar_raw.replace("\x00", "")

    def _metar_valid(self, metar: Metar.Metar) -> bool:
        return metar.time is not None

    def _temperature(self, metar: Metar.Metar) -> Optional[float]:
        if metar.temp is None:
            return None
        return metar.temp.value(units="C")

    def _dewpoint(self, metar: Metar.Metar) -> Optional[float]:
        if metar.dewpt is None:
            return None
        return metar.dewpt.value(units="C")

    def _relative_humidity(self, metar: Metar.Metar) -> Optional[float]:
        if metar.temp is None or metar.dewpt is None:
            return None
        return relative_humidity_from_dewpoint(
            metar.temp.value(units="C"),
            metar.dewpt.value(units="C"),
        )

    def _pressure(self, metar: Metar.Metar) -> Optional[float]:
        if metar.press is None:
            return None
        return metar.press.value(units="MB")

    def _pressure_sea(self, metar: Metar.Metar) -> Optional[float]:
        if metar.press_sea_level is None:
            return None
        return metar.press_sea_level.value(units="MB")

    def _wind_direction(self, metar: Metar.Metar) -> Optional[float]:
        if metar.wind_dir is None:
            return None
        return metar.wind_dir.value()

    def _wind_speed(self, metar: Metar.Metar) -> Optional[float]:
        if metar.wind_speed is None:
            return None
        return metar.wind_speed.value(units="KT")

    def _wind_gust(self, metar: Metar.Metar) -> Optional[float]:
        if metar.wind_gust is None:
            return None
        return metar.wind_gust.value(units="KT")


class AviationWeatherCenterMetarDownloader(MetarCsvDownloader):

    def __init__(
        self,
        stations: Dict[str, Station],
        target_dir: os.PathLike = "data",
    ):
        super().__init__(stations, target_dir)

    def download1(
        self,
        stations_to_search: List[Station],
        from_datetime: Optional[datetime] = None,
        hours: int = 0,
    ) -> bool:
        base_url = "https://www.aviationweather.gov/metar/data"
        params = {
            'ids': ",".join([s.code4 for s in stations_to_search]),
            'format': "raw",
            'hours': hours,
            'taf': "off",
            'layout': "off",
        }
        if isinstance(from_datetime, datetime):
            params['date'] = from_datetime.strftime("%Y%m%d%H%M")

        try:
            res = requests.get(base_url, params=params)
            res.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.error(
                f"Failed to fetch data from {base_url} "
                f"with params {params}. "
                f"Status code: {e.response.status_code}."
                f"Error: {e}"
            )
            return False
        except requests.exceptions.ConnectionError as e:
            logger.error(
                f"Failed to connect to {base_url} "
                f"with params {params}. "
                f"Error: {e}"
            )
            return False

        soup = BeautifulSoup(res.text, 'html.parser')
        for element in soup.find_all('code'):
            data = self._fetch_data_from_raw_metar(element.text)
            if data is not None:
                self.data.append(data)
        self._dump_data()

        return True


class WeatherGovMetarDownloader(MetarCsvDownloader):

    def __init__(
        self,
        stations: Dict[str, Station],
        target_dir: os.PathLike = "data",
    ):
        super().__init__(stations, target_dir)

    def download1(
        self,
        stations_to_search: List[Station],
        start_datetime: datetime,
        end_datetime: datetime,
    ) -> bool:
        if not isinstance(start_datetime, datetime):
            raise ValueError(
                f"start_datetime must "
                "be a datetime object, got {start_datetime}"
            )
        if not isinstance(end_datetime, datetime):
            raise ValueError(
                f"end_datetime must "
                "be a datetime object, got {end_datetime}"
            )
        if start_datetime > end_datetime:
            raise ValueError(
                f"start_datetime must be before end_datetime, "
                f"got {start_datetime} and {end_datetime}"
            )
        if start_datetime.month != end_datetime.month:
            raise ValueError(
                "start_datetime and end_datetime must be in the same month "
                "(each METAR contains only the day of month), "
                "otherwise METAR cannot be parsed correctly,"
                f"got {start_datetime} and {end_datetime}"
            )

        base_url = "https://api.mesowest.net/v2/stations/timeseries"
        params = {
            'STID': ",".join([s.code4 for s in stations_to_search]),
            'units': "english",
            'start': start_datetime.strftime("%Y%m%d%H%M"),
            'end': end_datetime.strftime("%Y%m%d%H%M"),
            'token': "d8c6aee36a994f90857925cea26934be",
            'complete': "1",
        }

        try:
            res = requests.get(base_url, params=params)
            res.raise_for_status()
            raw_json = res.json()
        except requests.exceptions.HTTPError as e:
            logger.error(
                f"Failed to fetch data from {base_url} "
                f"with params {params}. "
                f"Status code: {e.response.status_code}."
                f"Error: {e}"
            )
            return False
        except requests.exceptions.ConnectionError as e:
            logger.error(
                f"Failed to connect to {base_url} "
                f"with params {params}. "
                f"Error: {e}"
            )
            return False
        except requests.exceptions.JSONDecodeError as e:
            logger.error(
                f"Failed to decode JSON response from {base_url} "
                f"with params {params}. "
                f"Response: {res.text}. "
                f"Error: {e}"
            )
            return False
        except Exception as e:
            logger.error(
                f"Failed to fetch data from {base_url} "
                f"with params {params}. "
                f"Error: {e}"
            )
            return False

        raw_metars = []
        for station in raw_json['STATION']:
            if 'OBSERVATIONS' not in station:
                continue
            if 'metar_set_1' not in station['OBSERVATIONS']:
                continue
            raw_metars.extend(station['OBSERVATIONS']['metar_set_1'])

        for raw_metar in raw_metars:
            try:
                data = self._fetch_data_from_raw_metar(
                    raw_metar,
                    year=start_datetime.year,
                    month=start_datetime.month,
                )
            except Exception as e:
                logger.error(
                    "[FATAL] "
                    f"Failed to parse raw METAR {raw_metar}. "
                    f"Error: {e}"
                )
                continue
            if data is not None:
                self.data.append(data)

        self._dump_data()

        return True
