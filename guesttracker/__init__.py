from datetime import date  # noqa
from datetime import datetime as dt  # noqa
from datetime import timedelta as delta  # noqa
from typing import Union  # noqa

from guesttracker.config import AZURE_WEB, IS_QT_APP, SYS_FROZEN  # noqa

# force config to be imported first
if True:
    from jgutils.logger import getlog  # noqa

__version__ = '2.0.0'
VERSION = __version__


try:
    if SYS_FROZEN:
        raise Exception()

    from icecream import ic
    ic.configureOutput(prefix='')
except Exception as e:
    ic = lambda *args: print(*args)  # noqa

StrNone = Union[str, None]
