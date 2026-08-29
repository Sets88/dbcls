"""Tests for the 'c' copy key in dbcls.editor.InfoPopup.

Like the search tests, these drive ``handle_key`` directly with a fake
clipboard — ``draw`` is never called.
"""

from dbcls.editor import InfoPopup, K

ESC   = K(27)
COPY  = K(ord('c'))
SLASH = K(ord('/'))


class FakeClipboard:
    def __init__(self):
        self.text = None

    def copy(self, text):
        self.text = text


def _popup(text, clipboard=None, pages=None, visible=3):
    ip = InfoPopup(clipboard)
    ip.open('Info', pages if pages is not None else {'main': text})
    ip._inner_w = 60
    ip._visible = visible
    return ip


def test_copy_puts_page_text_on_clipboard():
    cb = FakeClipboard()
    ip = _popup('line one\nline two', cb)
    assert ip.handle_key(COPY) is None      # copying never closes the popup
    assert cb.text == 'line one\nline two'
    assert 'copied' in ip._copied_msg


def test_copy_strips_markup_and_fences():
    cb = FakeClipboard()
    ip = _popup('see -->>Details<<-- and `code`\n```\nSELECT 1\n```', cb)
    ip.handle_key(COPY)
    assert cb.text == 'see Details and code\nSELECT 1'


def test_copy_uses_current_page():
    cb = FakeClipboard()
    ip = _popup(None, cb, pages={'main': '-->>Details<<--', 'Details': 'inner text'})
    ip.handle_key(K(ord('\n')))             # follow the link
    ip.handle_key(COPY)
    assert cb.text == 'inner text'


def test_copy_on_link_page_does_not_close():
    cb = FakeClipboard()
    ip = _popup('-->>Details<<--', cb, pages={'main': '-->>Details<<--', 'Details': 'x'})
    assert ip.handle_key(COPY) is None      # unknown keys would close it
    assert cb.text == 'Details'


def test_copy_without_clipboard_is_reported():
    ip = _popup('text')
    assert ip.handle_key(COPY) is None
    assert ip._copied_msg == ' no clipboard '


def test_next_key_clears_the_copied_note():
    ip = _popup('text', FakeClipboard())
    ip.handle_key(COPY)
    ip.handle_key(SLASH)
    assert ip._copied_msg == ''


def test_c_is_typed_into_a_search_query():
    cb = FakeClipboard()
    ip = _popup('abc', cb)
    ip.handle_key(SLASH)
    ip.handle_key(COPY)
    assert ip._search_query == 'c'
    assert cb.text is None
