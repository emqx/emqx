"""Test cluster-join retries and expected failures without Docker or real sleeps."""

import subprocess
from pathlib import Path

import pytest


@pytest.fixture
def run_join():
    def run(docker_body, expected_error="", max_retries=1):
        script = f"""
source "$1"
shift
ready=false
docker() {{
    if [ "$*" = "logs node2" ]; then
        echo container-logs
        return 0
    fi
    if [ "$*" != "exec node2 emqx ctl cluster join node1" ]; then
        echo "unexpected docker arguments: $*" >&2
        return 1
    fi
    {docker_body}
}}
sleep() {{
    ready=true
    echo retry
}}
join_cluster "$@"
"""
        result = subprocess.run(
            [
                "bash",
                "-euc",
                script,
                "test-cluster-join",
                str(Path(__file__).with_name("cluster-join.sh")),
                "node2",
                "node1",
                str(max_retries),
                expected_error,
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.stderr == ""
        return result

    return run


def test_successful_join_does_not_retry(run_join):
    result = run_join("echo joined")

    assert result.returncode == 0
    assert result.stdout.splitlines() == ["joined"]


def test_expected_rejection_captures_stderr_without_retrying(run_join):
    result = run_join(
        "echo license-rejection >&2; return 1",
        expected_error="license-rejection",
    )

    assert result.returncode == 0
    assert result.stdout.splitlines() == [
        "Expected cluster join rejection: license-rejection"
    ]


@pytest.mark.parametrize("output", ["joined", "license-rejection"])
def test_successful_command_fails_negative_test(run_join, output):
    result = run_join(f"echo {output}", expected_error="license-rejection")

    assert result.returncode == 1
    assert result.stdout.splitlines() == [
        output,
        "ERROR: cluster join succeeded, expected: license-rejection",
    ]


@pytest.mark.parametrize(
    "output, expected_error",
    [
        pytest.param("unrelated-error", "license-rejection", id="unrelated-error"),
        pytest.param("license-rejection-extra", "license-rejection", id="substring"),
        pytest.param("licenseXrejection", "license.rejection", id="regex-metacharacter"),
    ],
)
def test_other_errors_do_not_pass_negative_test(run_join, output, expected_error):
    result = run_join(
        f"echo {output}; return 1",
        expected_error=expected_error,
        max_retries=0,
    )

    assert result.returncode == 1
    assert result.stdout.splitlines() == [
        output,
        "timeout waiting for cluster join to be accepted",
        "container-logs",
    ]


def test_positive_test_retries_boot_failure(run_join):
    result = run_join(
        'if "$ready"; then echo joined; else echo booting; return 1; fi'
    )

    assert result.returncode == 0
    assert result.stdout.splitlines() == ["booting", "retry", "joined"]


def test_negative_test_retries_boot_failure(run_join):
    result = run_join(
        'if "$ready"; then echo license-rejection; else echo booting; fi; return 1',
        expected_error="license-rejection",
    )

    assert result.returncode == 0
    assert result.stdout.splitlines() == [
        "booting",
        "retry",
        "Expected cluster join rejection: license-rejection",
    ]
