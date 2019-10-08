"""Functions to deal and handling  of time data."""
import datetime
import dateutil.tz


#############
# Functions #
#############

def is_later(timestamp, period):
    """Return true if the current time > timestamp + period.

    Checks if the timestamp given is later than the current time + the
    period given in seconds.

    Attributes:
    timestamp -- integer, EPOCH.
    period -- integer, seconds.

    """

    _timestamp_to_check = (
        datetime.datetime.fromtimestamp(timestamp, dateutil.tz.tzutc())
        + datetime.timedelta(seconds=period)
    )

    if now_in_utc() > _timestamp_to_check:
        return True
    else:
        return False


def mask_timestamp_by_delta(timestamp, delta=0):
    """Return false for timestamps later than the masking delta in days (UTC).

    Checks if the timestamp given is later than the current time +/- the
    delta given in days. When delta == 0, it uses the current date, but when
    delta != 0 the current date is normalized to today at 00:00 UTC before
    calculating the mask.

    Attributes:
    timestamp -- datetime aware time object.
    delta -- integer.

    """

    if delta == 0:
        _mask = now_in_utc()
    else:
        _mask = now_in_utc().replace(hour=0, minute=0, second=0, microsecond=0) \
            - datetime.timedelta(days=delta)

    if timestamp > _mask:
        return False
    else:
        return True


def now_in_utc():
    """Returns aware datetime object of current time in UTC."""

    return datetime.datetime.now(dateutil.tz.tzutc())
