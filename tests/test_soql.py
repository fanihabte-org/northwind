from __future__ import annotations

import pytest

from fakeforce.soql import Comparison, InComparison, SoqlSyntaxError, parse_soql


def test_parser_builds_a_typed_select_query() -> None:
    query = parse_soql(
        "SELECT Id, Name FROM Account "
        "WHERE Name LIKE 'Acme%' AND IsDeleted = FALSE "
        "ORDER BY Name DESC NULLS FIRST LIMIT 25 OFFSET 2"
    )

    assert query.fields == ("Id", "Name")
    assert query.object_name == "Account"
    assert query.predicates == (
        Comparison("Name", "LIKE", "Acme%"),
        Comparison("IsDeleted", "=", False),
    )
    assert query.order_by is not None
    assert query.order_by.direction == "DESC"
    assert query.order_by.nulls == "FIRST"
    assert query.limit == 25
    assert query.offset == 2


def test_parser_handles_in_and_escaped_strings() -> None:
    query = parse_soql("SELECT Id FROM Account WHERE Name IN ('O''Brien', 'Acme')")

    assert query.predicates == (InComparison("Name", ("O'Brien", "Acme")),)


def test_parser_accepts_scientific_numeric_literals() -> None:
    query = parse_soql("SELECT Id FROM Opportunity WHERE Amount >= 1e6")

    assert query.predicates == (Comparison("Amount", ">=", 1_000_000.0),)


@pytest.mark.parametrize(
    ("source", "message"),
    [
        ("FROM Account", "expected SELECT"),
        ("SELECT Id FROM Account OR Name = 'Acme'", "unexpected 'OR'"),
        ("SELECT Id FROM Account LIMIT -1", "LIMIT requires"),
        ("SELECT Id FROM Account WHERE Name IN ()", "expected literal"),
    ],
)
def test_parser_rejects_unsupported_or_invalid_syntax(source: str, message: str) -> None:
    with pytest.raises(SoqlSyntaxError, match=message):
        parse_soql(source)
