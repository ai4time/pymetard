import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict


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
    station_file_path: os.PathLike = Path(__file__).parent / "stations.txt",
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
        code4 = line[20:24].strip()
        if not code4 or len(code4) != 4:
            # Invalid code
            continue
        stations[code4] = Station(
            state_code=line[:2],
            name=line[3:19].strip(),
            code4=code4,
            latitude=_lat(line[39:45]),
            longitude=_lng(line[47:54]),
            elevation=float(line[55:59].strip()),
            raw=line,
        )
    return stations
