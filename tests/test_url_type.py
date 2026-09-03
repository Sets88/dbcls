"""The `g#` URL column type (dbcls.vd_modules.vd_types).

VisiData is a MagicMock under conftest, so what is exercised here is the part
that does not need it: the type constructor, the query-string parser, the cell
formatter (a URL cell must display exactly as it arrived) and the SQL literal a
parsed URL turns into on the edit sheet.
"""
import pytest

from dbcls.utils import UrlParts, prettify, sql_literal
from dbcls.vd_modules.vd_types import format_url_cell, parse_query, urltype


URL = 'https://user:pw@example.com:8443/a/b?x=1&y=two#frag'


class TestUrlType:
    def test_all_parts(self):
        assert urltype(URL) == {
            'schema': 'https',
            'domain': 'example.com',
            'port': 8443,
            'path': '/a/b',
            'query': {'x': '1', 'y': 'two'},
            'anchor': 'frag',
        }

    def test_part_order_is_the_column_order(self):
        assert list(urltype(URL)) == [
            'schema', 'domain', 'port', 'path', 'query', 'anchor']

    def test_missing_parts_are_none(self):
        assert urltype('http://example.com/') == {
            'schema': 'http',
            'domain': 'example.com',
            'port': None,
            'path': '/',
            'query': None,
            'anchor': None,
        }

    def test_relative_url(self):
        parts = urltype('/api/v1?a=1')
        assert (parts['schema'], parts['domain'], parts['path']) == (None, None, '/api/v1')
        assert parts['query'] == {'a': '1'}

    def test_scheme_relative_url(self):
        assert urltype('//cdn.example.com/x.js')['domain'] == 'cdn.example.com'

    def test_domain_is_lowercased_by_urlsplit(self):
        assert urltype('https://EXAMPLE.com/x')['domain'] == 'example.com'

    def test_non_numeric_port_does_not_raise(self):
        assert urltype('https://example.com:http/x')['port'] is None

    def test_bytes(self):
        assert urltype(b'https://example.com/%D0%B0?k=\xd0\xb0')['query'] == {'k': 'а'}

    def test_surrounding_whitespace_is_stripped(self):
        assert urltype('  https://example.com/x  ').url == 'https://example.com/x'

    def test_none_and_blank(self):
        assert urltype(None) is None
        assert urltype('') is None
        assert urltype('   ') is None

    def test_no_argument_is_the_default_value(self):
        """visidata calls type() with no args for a default value"""
        assert urltype() is None

    def test_already_parsed_passes_through(self):
        """an edited cell holds the value the previous type() call produced"""
        parts = urltype(URL)
        assert urltype(parts) is parts

    def test_not_a_url_raises(self):
        """visidata catches this and marks the cell as a typing error"""
        with pytest.raises(ValueError):
            urltype('hello world')

    def test_non_string_raises(self):
        with pytest.raises(ValueError):
            urltype(42)

    def test_sorting_works(self):
        """unlike jsontype: dict < dict is a TypeError, UrlParts compares by text"""
        urls = ['https://b.example.com/', 'https://a.example.com/']
        assert [p.url for p in sorted(urltype(u) for u in urls)] == sorted(urls)


class TestParseQuery:
    def test_no_query(self):
        assert parse_query('') is None
        assert parse_query(None) is None

    def test_single_values_stay_scalar(self):
        assert parse_query('a=1&b=2') == {'a': '1', 'b': '2'}

    def test_repeated_key_becomes_a_list(self):
        assert parse_query('a=1&a=2&a=3') == {'a': ['1', '2', '3']}

    def test_blank_value_is_kept(self):
        assert parse_query('a=&b=1') == {'a': '', 'b': '1'}

    def test_values_are_percent_decoded(self):
        assert parse_query('q=%D0%B0+%D0%B1') == {'q': 'а б'}

    def test_order_is_preserved(self):
        assert list(parse_query('z=1&a=2&m=3')) == ['z', 'a', 'm']


class TestFormatUrlCell:
    @pytest.mark.parametrize('url', [
        URL,
        'http://example.com/',
        '/api/v1?a=1',
        'https://example.com/x?q=%D0%B0#top',
    ])
    def test_cell_displays_the_url_unchanged(self, url):
        assert format_url_cell('', urltype(url)) == url


class TestSqlLiteral:
    def test_url_cell_round_trips_as_its_text(self):
        """not as JSON of the parts, which the dict branch would produce"""
        assert sql_literal(urltype(URL)) == f"'{URL}'"

    def test_quotes_are_doubled(self):
        assert sql_literal(urltype("https://example.com/?q=O'Hara")) == \
            "'https://example.com/?q=O''Hara'"


class TestPrettify:
    def test_url_cell_is_indented_json_of_its_parts(self):
        """`zf` on a URL cell shows what it was parsed into"""
        pretty = prettify(urltype('https://example.com/x?a=1'))
        assert '"schema": "https"' in pretty
        assert '"a": "1"' in pretty


class TestUrlParts:
    def test_is_a_dict(self):
        """visidata's expand-col dispatches on dict"""
        assert isinstance(urltype(URL), dict)

    def test_str_is_the_url(self):
        assert str(UrlParts('https://example.com/', {})) == 'https://example.com/'
