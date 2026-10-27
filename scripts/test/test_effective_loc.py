"""Test source selection with real Git repositories and cloc."""

import re
import shutil
import subprocess
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "effective-loc.sh"
pytestmark = pytest.mark.skipif(shutil.which("cloc") is None, reason="cloc is required")


def git(repo, *args):
    subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, timeout=10
    )


def source(repo, path, content):
    file = repo / path
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(content + "\n")


@pytest.fixture
def repo(tmp_path):
    repo = tmp_path / "repo with spaces"
    repo.mkdir()
    git(repo, "init", "-q")
    source(repo, "src/main.py", "root_value = 1")
    source(repo, "test/test_main.py", "assert root_value == 1")
    source(repo, "untracked.py", "untracked_root = 1")
    git(repo, "add", "src/main.py", "test/test_main.py")
    return repo


def add_dependencies(repo):
    git_dep = repo / "deps" / "git dep"
    git_dep.mkdir(parents=True)
    git(git_dep, "init", "-q")
    source(git_dep, "src/client source.py", "git_value = 2")
    source(git_dep, "test/test_client.py", "assert git_value == 2")
    source(git_dep, "untracked.py", "untracked_dependency = 2")
    git(git_dep, "add", "src/client source.py", "test/test_client.py")

    unpacked_dep = repo / "deps" / "unpacked dep"
    source(unpacked_dep, "src/client.py", "unpacked_value = 3")
    source(unpacked_dep, "test/test_client.py", "assert unpacked_value == 3")
    for dep in (git_dep, unpacked_dep):
        source(dep, "_build/generated.py", "generated_build = 4")
        source(dep, "ebin/generated.py", "generated_ebin = 5")


def count(repo, *options, cwd=None):
    args = ["bash", str(SCRIPT), *options]
    if cwd is None:
        args.append(str(repo))
    result = subprocess.run(
        args, cwd=cwd, capture_output=True, text=True, timeout=15
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stderr == ""
    assert "error:" not in result.stdout
    row = re.search(
        r"^Python\s+(\d+)\s+\d+\s+\d+\s+(\d+)\s*$", result.stdout, re.MULTILINE
    )
    assert row, result.stdout
    return tuple(map(int, row.groups()))


@pytest.mark.parametrize(
    "options, expected",
    [
        ([], (1, 1)),
        (["--with-tests"], (2, 2)),
        (["--with-deps"], (3, 3)),
        (["--with-deps", "--with-tests"], (6, 6)),
    ],
)
def test_dependency_and_test_selection(repo, options, expected):
    add_dependencies(repo)
    assert count(repo, *options) == expected


def test_missing_deps_directory(repo):
    assert count(repo, "--with-deps") == (1, 1)


def test_partially_populated_git_dependency(repo):
    add_dependencies(repo)
    dep = repo / "deps" / "git dep"
    source(dep, "absent.py", "absent_value = 6")
    git(dep, "add", "absent.py")
    (dep / "absent.py").unlink()
    assert count(repo, "--with-deps") == (3, 3)


def test_invocation_from_subdirectory(repo):
    add_dependencies(repo)
    assert count(repo, "--with-deps", cwd=repo / "src") == (3, 3)


def test_symlinked_dependencies(repo, tmp_path):
    add_dependencies(repo)
    installed = tmp_path / "installed deps"
    (repo / "deps").rename(installed)
    (repo / "deps").mkdir()
    for dep in installed.iterdir():
        (repo / "deps" / dep.name).symlink_to(dep, target_is_directory=True)
    assert count(repo, "--with-deps", "--with-tests") == (6, 6)
