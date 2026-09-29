"""The `g#` URL column type (dbcls.vd_modules.vd_types).

VisiData is a MagicMock under conftest, so what is exercised here is the part
that does not need it: the type constructor, the query-string parser, the cell
formatter (a URL cell must display exactly as it arrived) and the SQL literal a
parsed URL turns into on the edit sheet.
"""
import pytest

from dbcls.utils import UrlParts, prettify, sql_literal
from dbcls.vd_modules.vd_types import (
    format_url_cell, parse_query, query_with, url_with_part, url_with_parts, urltype,
)


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


class TestUrlWithPart:
    """Editing one part of a URL (`(` / `z Enter` on a `g#` column) replaces
    it in the text; nothing else is rebuilt from the parsed parts."""

    URL = 'https://user:pw@EXAMPLE.com:8080/a/b?q=hello+world&x=%7E1&flag&x=2#top'

    def test_port_keeps_user_info_and_the_case_of_the_host(self):
        assert url_with_part(self.URL, 'port', 9090).startswith('https://user:pw@EXAMPLE.com:9090/a/b?')

    def test_clearing_the_port_drops_the_colon(self):
        assert url_with_part(self.URL, 'port', None).startswith('https://user:pw@EXAMPLE.com/a/b?')

    def test_a_bad_port_is_refused(self):
        with pytest.raises(ValueError):
            url_with_part(self.URL, 'port', 'http')
        with pytest.raises(ValueError):
            url_with_part(self.URL, 'port', 70000)

    def test_domain(self):
        assert url_with_part(self.URL, 'domain', 'other.org').startswith('https://user:pw@other.org:8080/')

    def test_ipv6_host_keeps_its_brackets(self):
        assert url_with_part('http://[::1]:80/x', 'port', 81) == 'http://[::1]:81/x'
        assert url_with_part('http://h:80/x', 'domain', '::1') == 'http://[::1]:80/x'

    def test_path_schema_anchor(self):
        assert url_with_part('http://h/x', 'path', '/y') == 'http://h/y'
        assert url_with_part('http://h/x', 'schema', 'https') == 'https://h/x'
        assert url_with_part('http://h/x#a', 'anchor', None) == 'http://h/x'

    def test_an_unknown_part_is_refused(self):
        with pytest.raises(ValueError):
            url_with_part(self.URL, 'host', 'x')

    def test_url_with_parts_replaces_only_what_differs(self):
        parts = dict(urltype(self.URL))
        parts['path'] = '/c'
        assert str(url_with_parts(self.URL, parts)) == self.URL.replace('/a/b', '/c')


class TestQueryWith:
    RAW = 'q=hello+world&x=%7E1&flag&x=2'

    def test_unchanged_parameters_keep_their_text(self):
        params = parse_query(self.RAW)
        assert query_with(self.RAW, params) == self.RAW

    def test_only_the_changed_value_is_encoded(self):
        params = parse_query(self.RAW)
        params['x'] = ['~1', 'a b']
        assert query_with(self.RAW, params) == 'q=hello+world&x=%7E1&flag&x=a+b'

    def test_delete(self):
        params = parse_query(self.RAW)
        del params['flag']
        assert query_with(self.RAW, params) == 'q=hello+world&x=%7E1&x=2'

    def test_rename_stays_in_place(self):
        assert query_with('q=1&x=2', {'query': '1', 'x': '2'}) == 'query=1&x=2'

    def test_new_parameters_go_at_the_end(self):
        assert query_with('q=1', {'q': '1', 'n': None}) == 'q=1&n='

    def test_empty(self):
        assert query_with('', {'a': 'b'}) == 'a=b'
        assert query_with('a=b', None) == ''
