#!/usr/bin/env python3
"""Print the emqx.com download URLs for every package produced for a release.

Steering users to https://www.emqx.com/en/downloads/... (rather than the GitHub
release assets) keeps download statistics visible.

This script is pure enumeration: it never touches the network. It expands the
canonical build matrix (scripts/rel/build_matrix.py) into URLs, so it can run at
any point in the release cycle without secrets. It uses only the Python standard
library so CI does not need jq or yq installed.

Note: snap packages are published to the Snap Store, not to emqx.com, so they
are intentionally omitted here.

Usage:
  print-download-links.py <version>
  print-download-links.py --version <version> [--format text|markdown]
                          [--profile emqx-enterprise]

Examples:
  print-download-links.py 6.0.3
  print-download-links.py --version 6.0.3 --format markdown
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_matrix  # noqa: E402 (sibling module, path set above)

EDITIONS = {"emqx-enterprise": "enterprise"}


def linux_pkg_ext(os_token):
    """Native package extension for a linux os token.

    Mirrors the PKGERDIR logic in scripts/buildx.sh / build.
    """
    if os_token.startswith(("ubuntu", "debian", "raspbian")):
        return "deb"
    return "rpm"


def parse_args(argv):
    parser = argparse.ArgumentParser(
        add_help=True,
        usage="%(prog)s <version> [--format text|markdown] [--profile <profile>]",
    )
    parser.add_argument("version_pos", nargs="?", metavar="version")
    parser.add_argument("--version", dest="version_opt")
    parser.add_argument("--format", default="text", choices=["text", "markdown"])
    parser.add_argument(
        "--profile", default="emqx-enterprise", choices=list(EDITIONS)
    )
    args = parser.parse_args(argv)

    version = args.version_opt or args.version_pos
    if not version:
        parser.error("version is required")
    args.version = version
    return args


class UrlBuilder:
    def __init__(self, base_url, profile, version):
        self.base_url = base_url
        self.profile = profile
        self.version = version

    def pkg_url(self, os_token, arch, ext):
        # <os>-<arch>.<ext> style (linux/mac)
        return (
            f"{self.base_url}/{self.profile}-{self.version}-"
            f"{os_token}-{arch}.{ext}"
        )


def emit_text(matrix, urls):
    lines = []
    for row in matrix["linux"]:
        ext = linux_pkg_ext(row["os"])
        lines.append(urls.pkg_url(row["os"], row["arch"], ext))
        lines.append(urls.pkg_url(row["os"], row["arch"], "tar.gz"))
    for row in matrix["mac"]:
        lines.append(urls.pkg_url(row["os"], row["arch"], "zip"))
    return "\n".join(lines)


def md_link(label, url):
    return f"[{label}]({url})"


def emit_markdown(matrix, urls):
    lines = ["## Download", ""]

    def linux_bullet(row):
        ext = linux_pkg_ext(row["os"])
        pkg = md_link(f".{ext}", urls.pkg_url(row["os"], row["arch"], ext))
        tar = md_link(".tar.gz", urls.pkg_url(row["os"], row["arch"], "tar.gz"))
        return f"- `{row['os']}` ({row['arch']}): {pkg} — {tar}"

    lines.append("### Ubuntu / Debian")
    for row in matrix["linux"]:
        if row["os"].startswith(("ubuntu", "debian")):
            lines.append(linux_bullet(row))
    lines.append("")

    lines.append("### RHEL / Rocky / Amazon Linux")
    for row in matrix["linux"]:
        if row["os"].startswith(("el", "amzn")):
            lines.append(linux_bullet(row))
    lines.append("")

    lines.append("### macOS")
    for row in matrix["mac"]:
        link = md_link(".zip", urls.pkg_url(row["os"], row["arch"], "zip"))
        lines.append(f"- `{row['os']}` ({row['arch']}): {link}")

    return "\n".join(lines)


def main(argv):
    args = parse_args(argv)
    edition = EDITIONS[args.profile]
    base_url = f"https://www.emqx.com/en/downloads/{edition}/{args.version}"
    urls = UrlBuilder(base_url, args.profile, args.version)
    matrix = build_matrix.matrix()

    if args.format == "text":
        print(emit_text(matrix, urls))
    else:
        print(emit_markdown(matrix, urls))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
