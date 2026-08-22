from __future__ import annotations

import json

from pa_engine.market_sources.alpaca.stream import (
    AlpacaIEXClient,
    InMemoryWebSocket,
    RecordedWebSocketTransport,
    STREAM_URL,
)
from pa_engine.market_sources.alpaca.stream.smoke import run


def _socket():
    return InMemoryWebSocket(
        incoming=[
            '[{"T":"success","msg":"authenticated"}]',
            '[{"T":"subscription","trades":["AAPL"],"corrections":["AAPL"],"cancelErrors":["AAPL"]}]',
            '[{"T":"t","S":"AAPL","i":1,"x":"V","p":1.25,"s":2,"c":["@"],"t":"2026-03-09T13:30:00.1Z","z":"C"}]',
        ]
    )


def test_connect_auth_subscribe_and_control_events_are_confirmed_without_secret_output(capsys):
    socket = _socket()
    client = AlpacaIEXClient("AAPL", key_id="secret-id", secret_key="secret-value", transport=RecordedWebSocketTransport([socket]))
    status = client.connect()
    assert status.state == "ready"
    assert status.corrections_enabled and status.cancel_errors_enabled
    assert client.transport.urls == [STREAM_URL]
    assert json.loads(socket.sent[0]) == {"action": "auth", "key": "secret-id", "secret": "secret-value"}
    assert json.loads(socket.sent[1]) == {"action": "subscribe", "trades": ["AAPL"]}
    message = client.receive()
    assert message.update is not None and message.update.accepted
    assert "secret-id" not in json.dumps(status.to_dict())
    assert "secret-value" not in json.dumps(status.to_dict())


def test_missing_credentials_smoke_is_testable_and_secret_safe(capsys, monkeypatch):
    monkeypatch.delenv("APCA_API_KEY_ID", raising=False)
    monkeypatch.delenv("APCA_API_SECRET_KEY", raising=False)
    assert run(["AAPL"]) == 2
    output = capsys.readouterr().out
    assert "secret" not in output.lower()
    assert "APCA_API" not in output


def test_auth_error_is_typed_and_not_retried_as_data():
    socket = InMemoryWebSocket(incoming=['[{"T":"error","code":401,"msg":"bad secret"}]'])
    client = AlpacaIEXClient("AAPL", key_id="key", secret_key="secret", transport=RecordedWebSocketTransport([socket]))
    status = client.connect()
    assert status.state == "error"
    assert status.outcome.kind == "authentication"
    assert "bad secret" not in json.dumps(status.to_dict())
