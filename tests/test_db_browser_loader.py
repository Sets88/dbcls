"""The chunked loader of the table browser (dbcls.vd_modules.vd_db_browser).

The loader thread outlives the command that started it — it idles in the
throttle loop until the user scrolls — so it has to opt out of both of
VisiData's "wait for the previous work to finish" mechanisms, otherwise a
partially loaded sheet blocks commands for good.
"""
import threading
from unittest.mock import MagicMock

from dbcls.vd_modules.vd_db_browser import TableSampleDataSheet


class _Chunk:
    def __init__(self, data, has_more=False):
        self.data = data
        self.has_more = has_more


def _make_sheet(chunks):
    client = MagicMock()
    client.SUPPORTS_SERVER_SIDE_PAGING = False
    client.get_sample_data_sql.return_value = 'SELECT * FROM t'
    client.get_limit_sql.return_value = 'LIMIT 500'
    client.execute.side_effect = [_Chunk(c) for c in chunks]

    sheet = TableSampleDataSheet.__new__(TableSampleDataSheet)
    sheet.client = client
    sheet.db = 'db'
    sheet.table = 't'
    sheet.rows = []            # never reaches the "waiting to scroll" branch
    sheet.cursorRowIndex = 0
    sheet.progresses = []
    sheet.columns = []
    sheet.addColumn = MagicMock()
    return sheet


def _run_loader_in_thread(sheet):
    """Drain iterload() in its own thread and return that thread."""
    def _load():
        list(sheet.iterload())

    t = threading.Thread(target=_load)
    t.start()
    t.join(timeout=5)
    assert not t.is_alive()
    return t


class TestLoaderThreadOptOuts:
    def test_loader_is_not_counted_as_the_previous_command(self):
        # lastCommand=True would make every threaded command on the sheet fail
        # with "still running iterload from previous command".
        t = _run_loader_in_thread(_make_sheet([[{'a': 1}], []]))
        assert t.lastCommand is False

    def test_loader_does_not_block_vd_sync(self):
        # save/syscopy end up in a bare vd.sync() (save.py:
        # `vd.sync(*vd.ensureLoaded([]))`), which joins *every* unfinished
        # thread.  Without noblock that join never returns on a sheet that is
        # still loading: syscopyCells_async stays stuck in currentThreads and
        # every later command fails with
        # "still running syscopyCells_async from previous command".
        t = _run_loader_in_thread(_make_sheet([[{'a': 1}], []]))
        assert t.noblock is True
