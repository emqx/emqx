"""Test cluster-join retries and expected failures without Docker or real sleeps."""

import subprocess
from pathlib import Path

import pytest


@pytest.fixture
def run_join():
    script = Path(__file__).with_name("cluster-join-test.sh")

    def run(scenario):
        result = subprocess.run(
            ["bash", str(script), scenario],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.stderr == ""
        return result

    return run


def test_successful_join_does_not_retry(run_join):
    result = run_join("success")

    assert result.returncode == 0
    assert result.stdout.splitlines() == ["joined"]


def test_expected_rejection_captures_stderr_without_retrying(run_join):
    result = run_join("expected-rejection")

    assert result.returncode == 0
    assert result.stdout.splitlines() == [
        "Expected cluster join rejection: license-rejection"
    ]


@pytest.mark.parametrize(
    "scenario, output",
    [
        ("unexpected-success", "joined"),
        ("matching-output-success", "license-rejection"),
    ],
)
def test_successful_command_fails_negative_test(run_join, scenario, output):
    result = run_join(scenario)

    assert result.returncode == 1
    assert result.stdout.splitlines() == [
        output,
        "ERROR: cluster join succeeded, expected: license-rejection",
    ]


@pytest.mark.parametrize(
    "scenario, output",
    [
        ("unrelated-error", "unrelated-error"),
        ("substring-error", "license-rejection-extra"),
        ("regex-error", "licenseXrejection"),
    ],
)
def test_other_errors_do_not_pass_negative_test(run_join, scenario, output):
    result = run_join(scenario)

    assert result.returncode == 1
    assert result.stdout.splitlines() == [
        output,
        "timeout waiting for cluster join to be accepted",
        "container-logs",
    ]


def test_positive_test_retries_boot_failure(run_join):
    result = run_join("boot-then-success")

    assert result.returncode == 0
    assert result.stdout.splitlines() == ["booting", "retry", "joined"]


def test_negative_test_retries_boot_failure(run_join):
    result = run_join("boot-then-rejection")

    assert result.returncode == 0
    assert result.stdout.splitlines() == [
        "booting",
        "retry",
        "Expected cluster join rejection: license-rejection",
    ]
