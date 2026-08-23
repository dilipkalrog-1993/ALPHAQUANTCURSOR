import json

import pytest

from core.scoring_profiles import DEFAULT_WEIGHTS, ScoringProfileStore, simulate, validate_weights


def test_default_custom_version_activate_reset(tmp_path):
    store = ScoringProfileStore(tmp_path / "profiles.json")
    assert store.active().weights == DEFAULT_WEIGHTS
    custom = {"structure":35,"participation":25,"momentum":20,"market_sector":10,"historical":5,"news":5}
    store.save_new("Breakout Desk", custom)
    assert store.activate("Breakout Desk").version == 1
    v2 = store.save_version("Breakout Desk", custom)
    assert v2.version == 2 and store.active().version == 2
    assert store.reset().name == "AlphaQuant Default"


@pytest.mark.parametrize("weights", [
    {**DEFAULT_WEIGHTS, "news": 9},
    {**DEFAULT_WEIGHTS, "news": -1, "historical": 21},
    {**DEFAULT_WEIGHTS, "structure": 41, "news": -1},
])
def test_invalid_weights_rejected(weights):
    with pytest.raises(ValueError): validate_weights(weights)


def test_simulation_and_historical_snapshot(tmp_path):
    store=ScoringProfileStore(tmp_path/"profiles.json")
    old=store.active().snapshot("BREAKOUT")
    result=simulate({key:.5 for key in DEFAULT_WEIGHTS},DEFAULT_WEIGHTS)
    assert result["trade_confidence"] == 50
    custom={"structure":35,"participation":25,"momentum":20,"market_sector":10,"historical":5,"news":5}
    store.save_version("AlphaQuant Default",custom)
    assert old["version"] == 1 and old["weights"] == DEFAULT_WEIGHTS
    assert json.loads(json.dumps(old))["weights"]["news"] == 10
