# Creates a mock stub for RcsbDpUtiilty
import sys
import mock

sys.modules["wwpdb.utils.dp"] = mock.MagicMock()
sys.modules["wwpdb.utils.dp.RcsbDpUtility"] = mock.MagicMock()


class mocksetup(object):
    def __init__(self):
        pass
