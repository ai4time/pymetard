import os

import anylearn

from ingestion import fetch_stations, AwcWeatherStationDataDownloader


if os.environ.get('ANYLEARN_TASK_ID', None) is not None:
    data_workspace = anylearn.get_dataset("yhuang/MRMS").download()
else:
    data_workspace = "./output"


def run():
    stations = fetch_stations()
    downloader = AwcWeatherStationDataDownloader(target_dir=data_workspace)
    for station in stations.values():
        downloader.download1(station=station)


if __name__ == "__main__":
    run()
