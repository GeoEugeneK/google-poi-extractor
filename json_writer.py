import json
import os
# import threading
from queue import Empty

import multiprocessing as mp

import config
from dataclass import PoiData


class RawResponseWriter(mp.Process):

    __info_each_n: int = 1000

    def __init__(self, queue: mp.Queue, printlock: mp.Lock):

        self.file_encoding = config.DEFAULT_ENCODING
        self.file_extension = config.RESPONSE_JSON_EXTENSION
        if self.file_extension[0] != r".":
            self.file_extension = "." + self.file_extension

        self.data_dir = os.path.abspath(config.RAW_DATA_FOLDER)  # normalize and make absolute

        assert os.path.exists(self.data_dir), f"invalid directory for raw data JSONs \"{self.data_dir}\""

        self.__printlock = printlock
        self.queue = queue
        self.count = 0

        super().__init__(daemon=True, name="RawJsonWriterThread")

    def print(self, *args, **kwargs):
        with self.__printlock:
            print(*args, **kwargs)

    def write_poi_data(self, poi: PoiData):

        fp = self.data_dir + os.sep + poi.place_id + self.file_extension  # ext starts with a dot
        stringified: str = json.dumps(poi.json, indent=4)  # format JSON here

        with open(fp, 'w', encoding=self.file_encoding) as f:
            f.write(stringified)

        self.count += 1

    def run(self) -> None:

        self.print(f"{self.name}: writer started")

        done = False
        while not done:

            try:
                task = self.queue.get(timeout=5)   # don't wait forever
            except Empty:
                continue

            # terminate process if poison pill found
            if not isinstance(task, PoiData):
                self.print(f"{self.name} received poison pill")
                done = True
                break

            self.write_poi_data(task)

            if self.count % self.__info_each_n == 0:
                self.print(f"{self.name}: data for {self.count} POIs were written as JSONs.")

        self.print(f"{self.name}: writer done. Total {self.count} files written to disk. Exiting...")
