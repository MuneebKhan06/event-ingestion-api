import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from simulate_events import (  # noqa: E402
    parse_args,
    run_duplicate_check,
    wait_until_persisted,
)


class _FakeClient:
    """Scripts a sequence of POST statuses and GET results.

    `get_statuses` is consumed one call at a time, so a test can make the event
    appear only after several polls — the asynchronous behaviour the real
    consumer has.
    """

    def __init__(self, post_statuses: list[int], get_statuses: list[int]):
        self._post_statuses = list(post_statuses)
        self._get_statuses = list(get_statuses)
        self.post_calls = 0
        self.get_calls = 0

    def post(self, _path, **_kwargs):
        self.post_calls += 1
        return SimpleNamespace(status_code=self._post_statuses.pop(0))

    def get(self, _path, **_kwargs):
        self.get_calls += 1
        status = self._get_statuses.pop(0) if self._get_statuses else 404
        return SimpleNamespace(status_code=status)


def _args(**overrides) -> object:
    args = parse_args(["--duplicate"])
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def test_waits_for_the_event_to_appear_before_giving_up(monkeypatch):
    monkeypatch.setattr("simulate_events.time.sleep", lambda _s: None)
    client = _FakeClient(post_statuses=[], get_statuses=[404, 404, 200])

    assert wait_until_persisted(client, "some-id", timeout=5.0) is True
    assert client.get_calls == 3


def test_gives_up_after_the_timeout(monkeypatch):
    monkeypatch.setattr("simulate_events.time.sleep", lambda _s: None)
    ticks = iter([0.0, 1.0, 2.0, 3.0, 99.0])
    monkeypatch.setattr("simulate_events.time.monotonic", lambda: next(ticks))
    client = _FakeClient(post_statuses=[], get_statuses=[404, 404, 404, 404])

    assert wait_until_persisted(client, "some-id", timeout=5.0) is False


def test_reports_success_when_the_duplicate_is_rejected(monkeypatch, capsys):
    monkeypatch.setattr("simulate_events.time.sleep", lambda _s: None)
    client = _FakeClient(post_statuses=[202, 409], get_statuses=[200])

    assert run_duplicate_check(client, _args()) == 0
    assert "correctly rejected with 409" in capsys.readouterr().out


def test_fails_loudly_when_the_event_never_persists(monkeypatch, capsys):
    """A timeout must not be reported as a passing duplicate check.

    The old version printed "retry in a moment" and exited 0, so the flag could
    silently never demonstrate the thing it exists to demonstrate.
    """
    monkeypatch.setattr("simulate_events.time.sleep", lambda _s: None)
    ticks = iter([0.0, 1.0, 99.0])
    monkeypatch.setattr("simulate_events.time.monotonic", lambda: next(ticks))
    client = _FakeClient(post_statuses=[202], get_statuses=[404, 404])

    assert run_duplicate_check(client, _args()) == 1
    output = capsys.readouterr().out
    assert "could not be tested" in output
    # It must not have sent the duplicate — that result would be meaningless.
    assert client.post_calls == 1


def test_explains_a_202_when_the_precheck_is_disabled(monkeypatch, capsys):
    """202 after the first copy is stored is correct with the pre-check off."""
    monkeypatch.setattr("simulate_events.time.sleep", lambda _s: None)
    client = _FakeClient(post_statuses=[202, 202], get_statuses=[200])

    assert run_duplicate_check(client, _args()) == 0
    output = capsys.readouterr().out
    assert "ENABLE_DUPLICATE_PRECHECK=false" in output
    assert "no second row" in output


def test_unexpected_status_is_not_treated_as_success(monkeypatch, capsys):
    monkeypatch.setattr("simulate_events.time.sleep", lambda _s: None)
    client = _FakeClient(post_statuses=[202, 500], get_statuses=[200])

    assert run_duplicate_check(client, _args()) == 1
    assert "unexpected 500" in capsys.readouterr().out
