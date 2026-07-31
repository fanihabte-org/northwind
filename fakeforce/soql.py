"""A small typed parser for the supported Salesforce SOQL subset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


class SoqlSyntaxError(ValueError):
    """The query cannot be tokenized or parsed into the supported AST."""


@dataclass(frozen=True)
class Comparison:
    field: str
    operator: Literal["=", "!=", ">", ">=", "<", "<=", "LIKE"]
    value: Any


@dataclass(frozen=True)
class InComparison:
    field: str
    values: tuple[Any, ...]


Predicate = Comparison | InComparison


@dataclass(frozen=True)
class OrderBy:
    field: str
    direction: Literal["ASC", "DESC"] = "ASC"
    nulls: Literal["FIRST", "LAST"] = "LAST"


@dataclass(frozen=True)
class SelectQuery:
    fields: tuple[str, ...]
    object_name: str
    predicates: tuple[Predicate, ...] = ()
    order_by: OrderBy | None = None
    limit: int | None = None
    offset: int = 0


@dataclass(frozen=True)
class _Token:
    kind: str
    value: str
    position: int


_KEYWORDS = {
    "SELECT",
    "FROM",
    "WHERE",
    "AND",
    "IN",
    "LIKE",
    "ORDER",
    "BY",
    "ASC",
    "DESC",
    "NULLS",
    "FIRST",
    "LAST",
    "LIMIT",
    "OFFSET",
}
_PUNCTUATION = {",": "COMMA", "(": "LPAREN", ")": "RPAREN", "*": "STAR"}


def tokenize(source: str) -> tuple[_Token, ...]:
    """Tokenize strings, operators, identifiers, and scalar literals."""
    tokens: list[_Token] = []
    index = 0
    while index < len(source):
        char = source[index]
        if char.isspace():
            index += 1
            continue
        if char == "'":
            start = index
            index += 1
            value: list[str] = []
            while index < len(source):
                if source[index] != "'":
                    value.append(source[index])
                    index += 1
                    continue
                if index + 1 < len(source) and source[index + 1] == "'":
                    value.append("'")
                    index += 2
                    continue
                index += 1
                break
            else:
                raise SoqlSyntaxError(f"unterminated string at position {start}")
            tokens.append(_Token("STRING", "".join(value), start))
            continue
        if char in _PUNCTUATION:
            tokens.append(_Token(_PUNCTUATION[char], char, index))
            index += 1
            continue
        if char in "<>=!":
            start = index
            index += 1
            if index < len(source) and source[index] == "=":
                index += 1
            operator = source[start:index]
            if operator not in {"=", "!=", ">", ">=", "<", "<="}:
                raise SoqlSyntaxError(f"invalid operator {operator!r} at position {start}")
            tokens.append(_Token("OPERATOR", operator, start))
            continue
        start = index
        while index < len(source) and not source[index].isspace() and source[index] not in "',()*<>=!":
            index += 1
        value = source[start:index]
        if not value:
            raise SoqlSyntaxError(f"unexpected character {char!r} at position {index}")
        upper = value.upper()
        tokens.append(_Token("KEYWORD" if upper in _KEYWORDS else "WORD", upper if upper in _KEYWORDS else value, start))
    return tuple(tokens)


class _Parser:
    def __init__(self, tokens: tuple[_Token, ...]) -> None:
        self.tokens = tokens
        self.index = 0

    def parse(self) -> SelectQuery:
        self._expect_keyword("SELECT")
        fields = self._fields()
        self._expect_keyword("FROM")
        object_name = self._identifier("sObject name")
        predicates: list[Predicate] = []
        order_by: OrderBy | None = None
        limit: int | None = None
        offset = 0
        if self._accept_keyword("WHERE"):
            predicates.append(self._predicate())
            while self._accept_keyword("AND"):
                predicates.append(self._predicate())
        if self._accept_keyword("ORDER"):
            self._expect_keyword("BY")
            field = self._identifier("ORDER BY field")
            direction = "ASC"
            nulls = "LAST"
            if self._peek_keyword("ASC") or self._peek_keyword("DESC"):
                direction = self._advance().value
            if self._accept_keyword("NULLS"):
                token = self._expect_keyword("FIRST", "LAST")
                nulls = token.value
            order_by = OrderBy(field, direction, nulls)
        if self._accept_keyword("LIMIT"):
            limit = self._integer("LIMIT")
        if self._accept_keyword("OFFSET"):
            offset = self._integer("OFFSET")
        if self._peek() is not None:
            token = self._peek()
            raise SoqlSyntaxError(f"unexpected {token.value!r} at position {token.position}")
        return SelectQuery(fields, object_name, tuple(predicates), order_by, limit, offset)

    def _fields(self) -> tuple[str, ...]:
        if self._accept("STAR"):
            return ("*",)
        fields = [self._identifier("selected field")]
        while self._accept("COMMA"):
            fields.append(self._identifier("selected field"))
        return tuple(fields)

    def _predicate(self) -> Predicate:
        field = self._identifier("WHERE field")
        if self._accept_keyword("IN"):
            self._expect("LPAREN")
            values = [self._literal()]
            while self._accept("COMMA"):
                values.append(self._literal())
            self._expect("RPAREN")
            return InComparison(field, tuple(values))
        if self._accept_keyword("LIKE"):
            return Comparison(field, "LIKE", self._literal())
        operator = self._expect("OPERATOR").value
        return Comparison(field, operator, self._literal())

    def _literal(self) -> Any:
        token = self._advance()
        if token is None:
            raise SoqlSyntaxError("expected literal at end of query")
        if token.kind == "STRING":
            return token.value
        if token.kind != "WORD":
            raise SoqlSyntaxError(f"expected literal at position {token.position}")
        upper = token.value.upper()
        if upper == "NULL":
            return None
        if upper == "TRUE":
            return True
        if upper == "FALSE":
            return False
        try:
            return float(token.value) if "." in token.value else int(token.value)
        except ValueError:
            return token.value

    def _integer(self, clause: str) -> int:
        value = self._literal()
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise SoqlSyntaxError(f"{clause} requires a non-negative integer")
        return value

    def _identifier(self, description: str) -> str:
        token = self._expect("WORD")
        return token.value

    def _peek(self) -> _Token | None:
        return self.tokens[self.index] if self.index < len(self.tokens) else None

    def _peek_keyword(self, value: str) -> bool:
        token = self._peek()
        return token is not None and token.kind == "KEYWORD" and token.value == value

    def _accept_keyword(self, value: str) -> bool:
        if self._peek_keyword(value):
            self.index += 1
            return True
        return False

    def _expect_keyword(self, *values: str) -> _Token:
        token = self._advance()
        if token is None or token.kind != "KEYWORD" or token.value not in values:
            expected = " or ".join(values)
            position = "end of query" if token is None else f"position {token.position}"
            raise SoqlSyntaxError(f"expected {expected} at {position}")
        return token

    def _accept(self, kind: str) -> bool:
        token = self._peek()
        if token is not None and token.kind == kind:
            self.index += 1
            return True
        return False

    def _expect(self, kind: str) -> _Token:
        token = self._advance()
        if token is None or token.kind != kind:
            position = "end of query" if token is None else f"position {token.position}"
            raise SoqlSyntaxError(f"expected {kind} at {position}")
        return token

    def _advance(self) -> _Token | None:
        token = self._peek()
        if token is not None:
            self.index += 1
        return token


def parse_soql(source: str) -> SelectQuery:
    """Parse supported SOQL into a typed, immutable query AST."""
    return _Parser(tokenize(source)).parse()
