import pytest

from payments.messaging.retry import RetryPolicy


def test_three_attempts_mean_two_exponential_delays() -> None:
    policy = RetryPolicy(max_attempts=3, base_delay=2.0)

    assert policy.delays_ms == (2000, 4000)
    assert policy.delay_after(1) == 2000
    assert policy.delay_after(2) == 4000
    assert policy.delay_after(3) is None


def test_single_attempt_has_no_retries() -> None:
    policy = RetryPolicy(max_attempts=1, base_delay=1.0)

    assert policy.delays_ms == ()
    assert policy.delay_after(1) is None


@pytest.mark.parametrize(("max_attempts", "base_delay"), [(0, 1.0), (3, 0.0), (3, -1.0)])
def test_invalid_policy_is_rejected(max_attempts: int, base_delay: float) -> None:
    with pytest.raises(ValueError, match="must be"):
        RetryPolicy(max_attempts=max_attempts, base_delay=base_delay)
