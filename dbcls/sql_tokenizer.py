from kaa import doc_re
from kaa.syntax_highlight import (
    Keywords,
    SingleToken,
    Span,
    Token,
    Tokenizer,
)
from kaa.theme import Style


KEYWORDS = [
    'SELECT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 'COLUMN', 'USE',
    'FROM', 'JOIN', 'OUTER', 'INNER', 'LIMIT', 'ORDER BY', 'AS',
    'SHOW', 'FROM', 'WHERE', 'DESC', 'TABLES', 'CREATE', 'TABLE',
    'SET', 'IS', 'NOT', 'NULL', 'ON', 'IN', 'LIKE', 'ILIKE', 'AND',
    'OR', 'INSERT', 'INTO', 'VALUES', 'INTERVAL', 'GROUP', 'BY',
    'HAVING', 'GRANT', 'LEFT', 'RIGHT', 'FULL', 'CROSS'
]


sql_editor_themes = {
    'basic': [
        Style('string', 'Green', None, bold=True),
        Style('number', 'Yellow', None, bold=True),
    ]
}

class CaseInsensitiveKeywords(Keywords):
    def re_start(self) -> str:
        tokens = []
        for token in self._tokens:
            new_token = ''
            token = doc_re.escape(token)
            for char in token:
                if char.lower() != char.upper():
                    new_token += '[' + char.lower() + char.upper() + ']'
                    continue
                new_token = new_token + char
            if new_token:
                tokens.append(new_token)

        return rf'\b({"|".join(tokens)})\b'


class NonSqlComment(Span):
    pass


class CommandSpan(Span):
    pass

def sqleditor_tokens(sql_client) -> list[tuple[str, Token]]:
    return [
        ("directive", CommandSpan('directive', r'^(\s+)?\.', '(\s|;|$)')),
        ('comment1', Span('comment', r'-- ', '$')),
        ('comment2', NonSqlComment('comment', r'\#', '$')),
        ("string1", Span('string', '"', '"', escape='\\')),
        ("string2", Span('string', "'", "'", escape='\\')),
        ("number", SingleToken('number', [r'\b[0-9]+(\.[0-9]*)*\b', r'\b\.[0-9]+\b'])),
        ("keyword", CaseInsensitiveKeywords('keyword', sql_client.all_commands)),
        ("function", CaseInsensitiveKeywords('directive', sql_client.all_functions)),
    ]


def make_tokenizer(sql_client) -> Tokenizer:
    return Tokenizer(tokens=sqleditor_tokens(sql_client))
