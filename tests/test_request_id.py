import logging
import uuid
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

import app.api.routes.health as health_module
import app.main as main_module
from app.api.middleware import REQUEST_ID_HEADER, RequestIDFilter, request_id_var
from app.kafka.producer import producer as kafka_producer
from tests.test_api import _FakeEngine


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(kafka_producer, "start", AsyncMock())
    monkeypatch.setattr(kafka_producer, "stop", AsyncMock())
    # /health is the cheapest route that reaches application code.
    monkeypatch.setattr(health_module, "get_engine", lambda: _FakeEngine(reachable=True))
    with TestClient(main_module.app) as test_client:
        yield test_client


def test_generates_an_id_when_the_client_sends_none(client):
    response = client.get("/health")

    returned = response.headers[REQUEST_ID_HEADER]
    uuid.UUID(returned)  # raises if it isn't a well-formed id


def test_honours_a_client_supplied_id(client):
    """Lets a caller correlate this request with their own logs."""
    response = client.get("/health", headers={REQUEST_ID_HEADER: "abc-123"})

    assert response.headers[REQUEST_ID_HEADER] == "abc-123"


def test_each_request_gets_a_distinct_id(client):
    first = client.get("/health").headers[REQUEST_ID_HEADER]
    second = client.get("/health").headers[REQUEST_ID_HEADER]

    assert first != second


@pytest.mark.parametrize(
    "hostile",
    [
        "abc\ninfo:root:[-] fake log line",  # forged log entries
        "abc\rdef",
        "x" * 200,  # unbounded value bloating every record
        "id with spaces",
    ],
)
def test_rejects_unusable_client_ids(client, hostile):
    """Client-supplied text goes straight to the logs, so it isn't trusted.

    A newline would let a caller forge log lines. Rejected values are replaced
    with a generated id rather than silently rewritten, so nothing is echoed
    back that the caller didn't send and wouldn't recognise.
    """
    response = client.get("/health", headers={REQUEST_ID_HEADER: hostile})

    returned = response.headers[REQUEST_ID_HEADER]
    assert returned != hostile
    uuid.UUID(returned)
    assert "\n" not in returned and "\r" not in returned


def test_log_records_emitted_during_a_request_carry_its_id(client, monkeypatch, caplog):
    """The whole point: a log line can be traced back to one caller's request.

    Driven through the health probe's failure path, which is real application
    code that logs mid-request — the same shape as the 503 in POST /events that
    motivated this.
    """
    caplog.handler.addFilter(RequestIDFilter())
    monkeypatch.setattr(health_module, "get_engine", lambda: _FakeEngine(reachable=False))
    supplied = "trace-me-42"

    with caplog.at_level(logging.ERROR):
        client.get("/health", headers={REQUEST_ID_HEADER: supplied})

    probe_records = [r for r in caplog.records if "unreachable" in r.getMessage()]
    assert probe_records, "expected the health probe to log a failure"
    assert all(r.request_id == supplied for r in probe_records)


def test_ids_do_not_leak_into_logs_outside_a_request(client, caplog):
    caplog.handler.addFilter(RequestIDFilter())

    with caplog.at_level(logging.INFO):
        client.get("/health", headers={REQUEST_ID_HEADER: "inside-only"})
        logging.getLogger("app.test").info("emitted outside any request")

    outside = [r for r in caplog.records if r.getMessage() == "emitted outside any request"]
    assert outside and all(r.request_id == "-" for r in outside)


def test_request_id_is_reset_after_the_request(client):
    """A leaked id would mislabel whatever the worker handles next."""
    client.get("/health", headers={REQUEST_ID_HEADER: "leaky-id"})

    assert request_id_var.get() == "-"
