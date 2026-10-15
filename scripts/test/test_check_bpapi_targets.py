#!/usr/bin/env python3
"""
Pytest tests for scripts/check-bpapi-targets.exs.

Each test builds a small git repository with a BPAPI baseline, a proto module
at a release tag and the target module, then runs the script in it.

Usage:
    pytest scripts/test/test_check_bpapi_targets.py -v
"""

import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "check-bpapi-targets.exs"
DATA_DIR = "apps/emqx_bpapi/test/emqx_static_checks_data"

EMPTY_DUMP = "#{api => #{}, signatures => #{}, release => \"9.9\"}.\n"

# A baseline that records a call from `foo_proto_v1` to `foo:bar/1`.
FOO_DUMP = (
    "#{api => #{{foo, 1} => #{calls => [{{foo_proto_v1, bar, ['Node', 'A']},"
    " {foo, bar, ['A']}}], casts => []}}, signatures => #{}, release => \"9.9\"}.\n"
)

# A proto module that calls `foo:bar/1` and `foo:baz/0` through a macro.
FOO_PROTO = """\
-module(foo_proto_v1).
-export([bar/2, baz/1]).
-define(TARGET, foo).
bar(Node, A) ->
    erpc:call(Node, foo, bar, [A]).
baz(Nodes) ->
    erpc:multicall(Nodes, ?TARGET, baz, [], 5000).
"""


def foo_module(exports):
    return "-module(foo).\n-export([%s]).\n" % ", ".join(exports)


def git(repo, *args):
    subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@t", *args],
        cwd=repo,
        check=True,
        capture_output=True,
    )


def write(repo, path, content):
    file = repo / path
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(content)


def make_repo(tmp_path, dump, exports, proto=None):
    """Commits the tree, tags it 9.9.0, then deletes the proto module."""
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init", "-q")
    # asdf resolves `elixir` from this file; it stays untracked.
    tool_versions = ROOT / ".tool-versions"
    if tool_versions.exists():
        (repo / ".tool-versions").write_text(tool_versions.read_text())
    write(repo, f"{DATA_DIR}/9.9.bpapi3", dump)
    write(repo, "apps/foo/src/foo.erl", foo_module(exports))
    if proto is not None:
        write(repo, "apps/foo/src/proto/foo_proto_v1.erl", proto)
    git(repo, "add", "apps")
    git(repo, "commit", "-q", "-m", "release")
    git(repo, "tag", "9.9.0")
    if proto is not None:
        git(repo, "rm", "-q", "apps/foo/src/proto/foo_proto_v1.erl")
        git(repo, "commit", "-q", "-m", "drop proto")
    return repo


def run(repo, *args):
    return subprocess.run(
        [str(SCRIPT), *args], cwd=repo, capture_output=True, text=True
    )


def test_baseline_target_missing_fails(tmp_path):
    repo = make_repo(tmp_path, FOO_DUMP, exports=[])
    result = run(repo, "--no-tags")
    assert result.returncode == 1, result.stdout + result.stderr
    assert "foo v1 calls foo:bar/1, which this tree does not export" in result.stdout
    assert "9.9.bpapi3" in result.stdout


def test_baseline_target_exported_passes(tmp_path):
    repo = make_repo(tmp_path, FOO_DUMP, exports=["bar/1"])
    result = run(repo, "--no-tags")
    assert result.returncode == 0, result.stdout + result.stderr


def test_tag_target_missing_fails(tmp_path):
    """The baseline does not hold the proto module; the release tag does."""
    repo = make_repo(tmp_path, EMPTY_DUMP, exports=["bar/1"], proto=FOO_PROTO)
    result = run(repo)
    assert result.returncode == 1, result.stdout + result.stderr
    assert "foo v1 calls foo:baz/0, which this tree does not export" in result.stdout
    assert "called by: 9.9.0" in result.stdout
    assert "foo:bar/1" not in result.stdout


def test_tag_target_exported_passes(tmp_path):
    repo = make_repo(
        tmp_path, EMPTY_DUMP, exports=["bar/1", "baz/0"], proto=FOO_PROTO
    )
    result = run(repo)
    assert result.returncode == 0, result.stdout + result.stderr


def test_no_tags_fails(tmp_path):
    repo = make_repo(tmp_path, FOO_DUMP, exports=["bar/1"])
    git(repo, "tag", "-d", "9.9.0")
    result = run(repo)
    assert result.returncode == 1
    assert "No release tags found" in result.stderr


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
