import pytest

from alchemiscale.storage.cypher import cypher_list_from_scoped_keys


def test_cypher_list_from_scoped_keys():

    prefix = "FakeClass"
    org = "FakeOrg"
    campaign = "FakeCampaign"
    project = "FakeProject"

    sks = ["-".join((prefix, str(token), org, campaign, project)) for token in range(3)]

    expected = '["FakeClass-0-FakeOrg-FakeCampaign-FakeProject", "FakeClass-1-FakeOrg-FakeCampaign-FakeProject", "FakeClass-2-FakeOrg-FakeCampaign-FakeProject"]'

    assert cypher_list_from_scoped_keys(sks) == expected

    with pytest.raises(ValueError, match="`scoped_keys` must be a list of ScopedKeys"):
        cypher_list_from_scoped_keys(sks[0])
