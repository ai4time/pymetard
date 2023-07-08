import os
from datetime import datetime

import anylearn

from ingestion import (
    fetch_stations,
    AwcWeatherStationDataDownloader,
)


if os.environ.get('ANYLEARN_TASK_ID', None) is not None:
    data_workspace = anylearn.get_dataset("yhuang/METARs").download()
else:
    data_workspace = "./data"


def run():
    stations = fetch_stations() # 9318 stations
    downloader = AwcWeatherStationDataDownloader(
        stations=stations,
        target_dir=data_workspace,
    )

    # URL length limit ~8000
    # while each station = 4 digits code + 1 comma encoded in %2C
    CHUNK_SIZE = 100

    for i in range(0, len(stations.keys()), CHUNK_SIZE):
        candidates = list(stations.values())[i:i+CHUNK_SIZE]
        downloader.download1(
            stations_to_search=candidates,
            from_datetime=datetime.strptime("202307070000", "%Y%m%d%H%M"),
            hours=120, # Max 120 hours
        )


if __name__ == "__main__":
    run()
