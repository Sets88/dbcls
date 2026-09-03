"""The «b to close» hint dbcls puts in every sidebar title
(dbcls.vd_modules.vd_sidebar).

Only the pure rule is exercised: the wrappers around ``vd.drawSidebar`` and
``BaseSheet.drawSidebarText`` do nothing but decide *whether* the panel is
closable and hand the text to :func:`sidebar_title_with_hint`, which is where
the "split the heading off, append the hint" decision lives.
"""
from dbcls.vd_modules.vd_sidebar import CLOSE_HINT, add_hint, sidebar_title_with_hint


class TestSidebarTitleWithHint:
    def test_the_heading_becomes_the_title_and_carries_the_hint(self):
        text, title = sidebar_title_with_hint('# Chart\nplotext chart.\n')
        assert title == f'Chart · {CLOSE_HINT}'
        # the heading must leave the body: passing the title explicitly stops
        # VisiData from stripping it itself, and it would show up as text
        assert text == 'plotext chart.'

    def test_an_explicit_title_keeps_the_body_untouched(self):
        text, title = sidebar_title_with_hint('# not a heading here\nrows', title='Live sheet')
        assert title == f'Live sheet · {CLOSE_HINT}'
        assert text == '# not a heading here\nrows'

    def test_a_panel_without_a_heading_gets_the_hint_as_its_whole_title(self):
        # e.g. the default_sidebar built from options.disp_sidebar_fmt
        text, title = sidebar_title_with_hint('12 rows\n3 columns')
        assert title == CLOSE_HINT
        assert text == '12 rows\n3 columns'

    def test_a_guide_indented_in_a_class_body_is_dedented(self):
        text, title = sidebar_title_with_hint('\n    # Tables\n    - `Enter` to open.\n')
        assert title == f'Tables · {CLOSE_HINT}'
        assert text == '- `Enter` to open.'

    def test_the_hint_is_not_added_twice(self):
        _, title = sidebar_title_with_hint('# Chart\nplotext chart.')
        _, title = sidebar_title_with_hint(f'# {title}\nplotext chart.')
        assert title == f'Chart · {CLOSE_HINT}'

    def test_an_empty_panel_gets_no_hint(self):
        # VisiData returns before drawing the frame, so a title would be lost
        assert sidebar_title_with_hint('') == ('', '')
        assert sidebar_title_with_hint('# Chart\n') == ('', 'Chart')


class TestAddHint:
    def test_appends_once(self):
        assert add_hint('Chart') == f'Chart · {CLOSE_HINT}'
        assert add_hint(add_hint('Chart')) == f'Chart · {CLOSE_HINT}'

    def test_no_title_is_the_hint_alone(self):
        assert add_hint('') == CLOSE_HINT
