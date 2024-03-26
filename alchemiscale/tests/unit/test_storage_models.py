import pytest

from alchemiscale.storage.models import NetworkStateEnum, NetworkMark
from alchemiscale import ScopedKey


class TestNetworkState(object):

    network_sk = ScopedKey.from_str(
        "AlchemicalNetwork-foo123-FakeOrg-FakeCampaign-FakeProject"
    )

    @pytest.mark.parametrize(
        ("state", "should_fail"),
        [
            ("active", False),
            ("inactive", False),
            ("invalid", False),
            ("deleted", False),
            ("Active", True),
            ("NotAState", True),
        ],
    )
    def test_enum_values(self, state: str, should_fail: bool):

        def create_network_state():
            return NetworkMark(self.network_sk, state)

        if should_fail:
            with pytest.raises(ValueError, match="`state` = "):
                create_network_state()
        else:
            create_network_state()

    def test_suggested_states_message(self):
        try:
            NetworkMark(self.network_sk, "NotAState")
        except ValueError as e:
            emessage = str(e)
            suggested_states = emessage.split(":")[1].strip().split(", ")
            assert len(suggested_states) == len(NetworkStateEnum)
            for state in suggested_states:
                NetworkStateEnum(state)
