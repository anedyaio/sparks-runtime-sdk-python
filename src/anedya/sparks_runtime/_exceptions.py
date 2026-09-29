"""
anedya_sparks._exceptions
~~~~~~~~~~~~~~~~~~~~~~~~~

SDK-specific exception types.
"""


class SparksError(Exception):
    """Base class for all anedya_sparks SDK errors."""


class SparksConnectionError(SparksError):
    """Raised when the gRPC connection to the Sparks runtime fails."""


class SparksHandlerError(SparksError):
    """
    Raised when the user's handler function raises an unhandled exception.
    Wraps the original exception so the SDK can report it to the error endpoint.
    """

    def __init__(self, original: BaseException) -> None:
        self.original = original
        super().__init__(str(original))


class SparksConfigError(SparksError):
    """Raised when required configuration (env vars, arguments) is missing or invalid."""


# TODO: Add SparksTaskErrorReportFailed once the error endpoint is defined in the proto.
#       This exception will be raised when the SDK cannot successfully deliver the error
#       report to the runtime after a handler failure.
