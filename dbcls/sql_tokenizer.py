from kaa.theme import Style
from kaa.syntax_highlight import Span, SingleToken, Tokenizer, Keywords, Token


sql_editor_themes = {
    'basic': [
        Style('string', 'Green', None, bold=True),
        Style('number', 'Yellow', None, bold=True),
    ]
}


KEYWORDS = [
    'SELECT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 'COLUMN', 'USE',
    'FROM', 'JOIN', 'OUTER', 'INNER', 'LIMIT', 'ORDER BY', 'AS',
    'SHOW', 'FROM', 'WHERE', 'DESC', 'TABLES', 'CREATE', 'TABLE',
    'SET', 'IS', 'NOT', 'NULL', 'ON', 'IN', 'LIKE', 'ILIKE', 'AND',
    'OR', 'INSERT', 'INTO', 'VALUES', 'INTERVAL', 'GROUP', 'BY',
    'HAVING', 'GRANT', 'LEFT', 'RIGHT', 'FULL', 'CROSS'
]


def sqleditor_tokens() -> list[tuple[str, Token]]:
    return [
        ('comment1', Span('comment', r'--', '$')),
        ('comment2', Span('comment', r'\#', '$')),
        ("string1", Span('string', '"', '"', escape='\\')),
        ("string2", Span('string', "'", "'", escape='\\')),
        ("number", SingleToken('number', [r'\b[0-9]+(\.[0-9]*)*\b', r'\b\.[0-9]+\b'])),
        ("keyword", Keywords('keyword', KEYWORDS)),
    ]


def make_tokenizer() -> Tokenizer:
    return Tokenizer(tokens=sqleditor_tokens())
