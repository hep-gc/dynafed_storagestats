"""Helper functions used by the other modules."""

import logging
import logging.handlers


#############
#  Classes  #
#############

class ContextFilter(logging.Filter):
    """
    This is a filter which injects contextual information into the log.
    """
    def __init__(self, logid):
        """Create ContextFilter with extra attributes for logging.

        Arguments:
        logid -- string.

        """
        self.logid = logid

    def filter(self, record):
        record.logid = self.logid
        return True


#############
# Functions #
#############

def setup_logger(logfile="/tmp/dynafed_storagestats.log", logid=False, loglevel="WARNING", verbose=False):
    """Setup the logger format to be used throughout the script.

    Arguments:
    logfile -- string defining path to write logs to.
    logid -- string defining an ID to be logged.
    loglevel -- string defining level to log: "DEBUG, INFO, WARNING, ERROR"
    verbose -- boolean. 'True' prints log messages to stderr.

    """
    # To capture warnings emitted by modules.
    logging.captureWarnings(True)

    # Create file logger.
    _logger = logging.getLogger("dynafed_storagestats")

    # Set log level to use.
    _num_loglevel = getattr(logging, loglevel.upper())
    _logger.setLevel(_num_loglevel)

    # Set file where to log and the mode to use and set the format to use.
    _log_handler_file = logging.handlers.TimedRotatingFileHandler(
        logfile,
        when="midnight",
        backupCount=15,
    )

    # Set the format depending whether a log id is requested.
    if logid:
        # Add ContextFilter
        _logid_context = ContextFilter(logid)

        # Add logid filter.
        _log_handler_file.addFilter(_logid_context)

        # Set logger format
        _log_format_file = logging.Formatter('%(asctime)s - [%(logid)s] - [%(levelname)s]%(message)s')

    else:
        # Set logger format
        _log_format_file = logging.Formatter('%(asctime)s - [%(levelname)s]%(message)s')

    # Set the format to the file handler.
    _log_handler_file.setFormatter(_log_format_file)

    # Add the file handler.
    _logger.addHandler(_log_handler_file)

    # Create STDERR handler if verbose is requested and add it to logger.
    if verbose:

        log_handler_stderr = logging.StreamHandler()

        if logid:
            log_format_stderr = logging.Formatter('%(asctime)s - [%(logid)s] - [%(levelname)s]%(message)s')
            log_handler_stderr.addFilter(_logid_context)
        else:
            log_format_stderr = logging.Formatter('%(asctime)s - [%(levelname)s]%(message)s')

        log_handler_stderr.setLevel(_num_loglevel)
        log_handler_stderr.setFormatter(log_format_stderr)
        # Add handler
        _logger.addHandler(log_handler_stderr)
