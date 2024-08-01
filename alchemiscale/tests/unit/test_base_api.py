import pytest

from fastapi import HTTPException
from pydantic.v1 import ValidationError

from alchemiscale.base.api import validate_scopes, validate_scopes_query
from alchemiscale.models import Scope, ScopedKey
from alchemiscale.security.models import Token, TokenData, CredentialedEntity


@pytest.mark.parametrize(
    "scope", (("a-", "a", "a"), ("a", "a-", "a"), ("a", "a", "a-"))
)
@pytest.mark.parametrize("scope_cls", [Scope, ScopedKey])
def test_create_invalid_scope_with_dash(scope_cls, scope):
    org, campaign, project = scope
    with pytest.raises(ValidationError):
        scope_cls(org=org, campaign=campaign, project=project)


@pytest.fixture
def tokendata():
    return TokenData(
        entity="not-a-real-user",
        scopes=[
            "org1-campaignA-projectI",
            "org1-campaignB-projectI",
            "org1-campaignB-projectII",
            "org2-*-*",
            "org3-campaignA-*",
        ],
    )


@pytest.mark.parametrize(
    "scope_str",
    [
        "org1-campaignA-projectI",
        "org1-campaignB-projectI",
        "org1-campaignB-projectII",
        "org2-campaignB-projectII",
        "org3-campaignA-projectII",
    ],
)
def test_validate_scopes_valid(tokendata, scope_str):
    assert validate_scopes(Scope.from_str(scope_str), tokendata) is None


@pytest.mark.parametrize(
    "scope_str",
    [
        "org4-campaignB-projectII",
        "org3-campaignB-projectII",
        "org1-campaignB-projectIII",
    ],
)
def test_validate_scopes_invalid(tokendata, scope_str):
    with pytest.raises(HTTPException):
        validate_scopes(Scope.from_str(scope_str), tokendata)


@pytest.mark.parametrize(
    "scope_str,expected",
    [
        ("org1-campaignB-projectI", ["org1-campaignB-projectI"]),
        ("org1-campaignA-projectI", ["org1-campaignA-projectI"]),
        ("org1-campaignB-*", ["org1-campaignB-projectI", "org1-campaignB-projectII"]),
        (
            "org1-*-*",
            [
                "org1-campaignA-projectI",
                "org1-campaignB-projectI",
                "org1-campaignB-projectII",
            ],
        ),
        (
            "*-*-*",
            [
                "org1-campaignA-projectI",
                "org1-campaignB-projectI",
                "org1-campaignB-projectII",
                "org2-*-*",
                "org3-campaignA-*",
            ],
        ),
        ("org2-*-*", ["org2-*-*"]),
        ("org3-campaignA-*", ["org3-campaignA-*"]),
        ("org1-campaignB-projectIII", []),
        ("org2-campaignC-*", ["org2-campaignC-*"]),
        ("org4-*-*", []),
    ],
)
def test_validate_scopes_query(tokendata, scope_str, expected):
    expected_scopes = [Scope.from_str(s) for s in expected]
    assert set(validate_scopes_query(Scope.from_str(scope_str), tokendata)) == set(
        expected_scopes
    )
    assert set(
        validate_scopes_query(Scope.from_str(scope_str), tokendata, as_str=True)
    ) == set(expected)
