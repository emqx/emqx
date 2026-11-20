"""Check release workflow decisions against GA and Cloud tag events."""

import re
from pathlib import Path
from types import SimpleNamespace

WORKFLOWS = Path(__file__).resolve().parents[2] / ".github" / "workflows"


def job(name, workflow):
    text = (WORKFLOWS / workflow).read_text()
    match = re.search(rf"(?ms)^  {re.escape(name)}:\n(.*?)(?=^  [\w-]+:|\Z)", text)
    assert match, f"job {name} not found in {workflow}"
    return match.group(1)


def expression(block, key):
    match = re.search(rf"(?m)^(\s+){re.escape(key)}: >-\n((?:\s+.*\n)+)", block)
    assert match, f"{key} expression not found"
    indent = len(match.group(1))
    lines = []
    for line in match.group(2).splitlines():
        if line.strip() and len(line) - len(line.lstrip()) <= indent:
            break
        lines.append(line.strip())
    return " ".join(lines).removeprefix("${{").removesuffix("}}").strip()


def evaluate(condition, event, tag, prerelease=False, publish=False, task="build"):
    github = SimpleNamespace(
        event_name=event,
        event=SimpleNamespace(release=SimpleNamespace(tag_name=tag, prerelease=prerelease)),
    )
    inputs = SimpleNamespace(tag=tag, publish_release_artifacts=publish, task=task)
    condition = condition.replace("&&", " and ").replace("||", " or ")
    condition = re.sub(r"!(?!=)", " not ", condition).strip()
    return bool(eval(condition, {"__builtins__": {}}, {
        "github": github,
        "inputs": inputs,
        "contains": lambda value, part: part in value,
    }))


def test_snap_verification_and_prepare_use_same_release_gate():
    verify = expression(job("verify-release-tag", "build_snap.yaml"), "if")
    prepare = expression(job("prepare", "build_snap.yaml"), "if")
    for tag, prerelease, expected in [
        ("6.1.6", False, True),
        ("6.1.6-rc.1", True, False),
        ("6.1.6-cloud.1", False, False),
        ("6.1.6-preview.1", False, False),
    ]:
        assert evaluate(verify, "release", tag, prerelease) is expected
        assert evaluate(prepare, "release", tag, prerelease) is expected


def test_release_verification_preserves_cloud_upload_and_ga_gate():
    verify = expression(job("verify-release-tag", "release.yaml"), "require_ga")
    publish = expression(job("upload", "release.yaml"), "if")
    for tag, prerelease, expected in [
        ("6.1.6", False, True),
        ("6.1.6-rc.1", True, False),
        ("6.1.6-cloud.1", False, False),
        ("6.1.6-preview.1", False, False),
    ]:
        assert evaluate(verify, "release", tag, prerelease) is expected
        assert evaluate(publish, "release", tag, prerelease) is expected
    # An explicit publication request still requires a GA tag.
    assert evaluate(verify, "workflow_dispatch", "6.1.6-cloud.1", publish=True)
    assert not evaluate(publish, "workflow_dispatch", "6.1.6-cloud.1", publish=True)
