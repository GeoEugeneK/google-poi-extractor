class InvalidPoiDataError(Exception):
    """ Gets raised when PoiData instance cannot be created. """


class ResponseParsingError(Exception):
    """ Must be raised when response parsing is impossible. """


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