import json
import os
import threading
from queue import Empty, Queue

import config
from dataclass import PoiData


class RawResponseWriter(threading.Thread):

    __info_each_n: int = 1000

    def __init__(self, tasks_queue: Queue, lock: threading.Lock):

        self.file_encoding = config.DEFAULT_ENCODING
        self.file_extension = config.RESPONSE_JSON_EXTENSION
        if self.file_extension[0] != r".":
            self.file_extension = "." + self.file_extension

        self.data_dir = os.path.abspath(config.RAW_DATA_FOLDER)  # normalize and make absolute

        assert os.path.exists(self.data_dir), f"invalid directory for raw data JSONs \"{self.data_dir}\""

        self.lock = lock
        self.queue = tasks_queue
        self.count = 0

        super().__init__(daemon=True, name="RawJsonWriterThread")

    def write_poi_data(self, poi: PoiData):

        fp = self.data_dir + os.sep + poi.place_id + self.file_extension  # ext starts with a dot
        stringified: str = json.dumps(poi.json, indent=4)  # format JSON here

        with open(fp, 'w', encoding=self.file_encoding) as f:
            f.write(stringified)

        self.count += 1

    def run(self) -> None:

        with self.lock:
            print(f"{self.name}: writer started")

        done = False
        while not done:

            try:
                task = self.queue.get(timeout=5)   # don't wait forever
            except Empty:
                continue

            # terminate process if poison pill found
            if not isinstance(task, PoiData):
                done = True
                break

            self.write_poi_data(task)

            if self.count % self.__info_each_n == 0:
                with self.lock:
                    print(f"{self.name}: data for {self.count} POIs were written as JSONs.")

        with self.lock:
            print(f"{self.name}: writer done. Total {self.count} files written to disk. Exiting...")
