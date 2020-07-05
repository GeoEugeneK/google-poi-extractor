import traceback
from typing import List
import datetime


class InvalidPoiDataError(Exception):
    """ Gets raised when PoiData instance cannot be created. """


class ResponseParsingError(Exception):
    """ Must be raised when response parsing is impossible. """


class SearchRecursionError(Exception):
    """ Raise when search radius gets too small. """


class ZeroResultsException(Exception):
    pass


class WastedQuotaException(Exception):
    pass


class RequestDeniedException(Exception):
    pass


class InvalidRequestException(Exception):
    pass


class FinishException(Exception):
    pass


def write_traceback(e: Exception, file: str, append: bool = True):

    """ Writes traceback to a file in specified mode."""

    calls: List[str] = traceback.format_tb(e.__traceback__)
    formatted_tb = "".join(calls)

    timemark = datetime.datetime.now().strftime("%b %d, %H-%M-%S")

    mode = "a" if append else "w"
    with open(file, mode, encoding="utf-8-sig") as f:
        f.write(f"\n\n\n{timemark}:   TRACEBACK\n" + formatted_tb + str(e))
