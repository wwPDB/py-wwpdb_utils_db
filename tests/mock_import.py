# Creates a mock stub for RcsbDpUtiilty
import sys
from unittest import mock

sys.modules["wwpdb.utils.dp"] = mock.MagicMock()
sys.modules["wwpdb.utils.dp.RcsbDpUtility"] = mock.MagicMock()


class mocksetup:
    def __init__(self):
        pass
