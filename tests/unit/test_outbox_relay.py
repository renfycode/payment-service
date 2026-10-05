from payments.messaging.outbox_relay import OutboxRelay


def make_relay(poll_interval: float, max_backoff: float) -> OutboxRelay:
    return OutboxRelay(
        session_factory=None,  # type: ignore[arg-type]  # backoff не обращается к БД
        broker=None,  # type: ignore[arg-type]
        batch_size=100,
        poll_interval=poll_interval,
        max_backoff=max_backoff,
    )


def test_backoff_grows_exponentially_and_is_capped() -> None:
    relay = make_relay(poll_interval=1.0, max_backoff=30.0)

    assert [relay.backoff(n) for n in range(1, 8)] == [2, 4, 8, 16, 30, 30, 30]
