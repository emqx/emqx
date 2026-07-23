#!/usr/bin/env python3
"""Print the download URLs for every package produced for a release.

Steering users to https://www.emqx.com/downloads/... (rather than the GitHub
release assets) keeps download statistics visible.

This script is pure enumeration: it never touches the network. It reads the
checked-out tree -- the release version (./pkg-vsn.sh --release), the platform
matrix (scripts/rel/build_matrix.py), and the plugin versions -- and expands
them into URLs, so it can run at any point in the release cycle without secrets.
It uses only the Python standard library so CI does not need jq or yq installed.

Two families of URLs are printed:
  - EMQX packages, served from the emqx.com download CDN.
  - Plugin packages, which are NOT on the CDN -- they are fetched straight from
    S3 (https://packages.emqx.io/emqx-plugins/...). Each plugin under plugins/
    ships a VERSION file that gives its package version.

Note: snap packages are published to the Snap Store, not to emqx.com, so they
are intentionally omitted here.

Usage:
  print-download-links.py [version]
  print-download-links.py [--version <version>] [--format text|markdown]
                          [--profile emqx-enterprise]

The version defaults to ./pkg-vsn.sh --release when omitted.

Examples:
  print-download-links.py
  print-download-links.py 6.0.3
  print-download-links.py --format markdown
"""

import argparse
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_matrix  # noqa: E402 (sibling module, path set above)

REPO_ROOT = Path(__file__).resolve().parents[2]
EDITIONS = {"emqx-enterprise": "enterprise"}

# Plugin packages are downloaded straight from S3, not the emqx.com CDN.
PLUGINS_BASE_URL = "https://packages.emqx.io/emqx-plugins"


def linux_pkg_ext(os_token):
    """Native package extension for a linux os token.

    Mirrors the PKGERDIR logic in scripts/buildx.sh / build.
    """
    if os_token.startswith(("ubuntu", "debian", "raspbian")):
        return "deb"
    return "rpm"


def release_version():
    """The release version of the checked-out tree, via ./pkg-vsn.sh --release."""
    return subprocess.run(
        [str(REPO_ROOT / "pkg-vsn.sh"), "--release"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def parse_args(argv):
    parser = argparse.ArgumentParser(
        add_help=True,
        usage="%(prog)s [version] [--format text|markdown] [--profile <profile>]",
    )
    parser.add_argument(
        "version_pos",
        nargs="?",
        metavar="version",
        help="release version; defaults to ./pkg-vsn.sh --release",
    )
    parser.add_argument("--version", dest="version_opt")
    parser.add_argument("--format", default="text", choices=["text", "markdown"])
    parser.add_argument(
        "--profile", default="emqx-enterprise", choices=list(EDITIONS)
    )
    args = parser.parse_args(argv)

    args.version = args.version_opt or args.version_pos or release_version()
    return args


def plugin_rows(repo_root):
    """(name, version) for every plugin under plugins/ that ships a VERSION file.

    Sorted by plugin name for stable output.
    """
    plugins_dir = repo_root / "plugins"
    rows = []
    for version_file in sorted(plugins_dir.glob("*/VERSION")):
        name = version_file.parent.name
        version = version_file.read_text().strip()
        rows.append((name, version))
    return rows


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

    def plugin_url(self, name, plugin_version):
        # <name>-<plugin_version>.tar.gz under the release-version directory.
        return (
            f"{PLUGINS_BASE_URL}/{self.version}/{name}-{plugin_version}.tar.gz"
        )


def emit_text(matrix, plugins, urls):
    lines = []
    for row in matrix["linux"]:
        ext = linux_pkg_ext(row["os"])
        lines.append(urls.pkg_url(row["os"], row["arch"], ext))
        lines.append(urls.pkg_url(row["os"], row["arch"], "tar.gz"))
    for row in matrix["mac"]:
        lines.append(urls.pkg_url(row["os"], row["arch"], "zip"))
    for name, version in plugins:
        lines.append(urls.plugin_url(name, version))
    return "\n".join(lines)


def md_link(label, url):
    return f"[{label}]({url})"


def emit_markdown(matrix, plugins, urls):
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

    if plugins:
        lines.append("")
        lines.append("### Plugins")
        # Plugin packages are always .tar.gz, so label the link by name-version.
        for name, version in plugins:
            link = md_link(f"{name}-{version}", urls.plugin_url(name, version))
            lines.append(f"- {link}")

    return "\n".join(lines)


def main(argv):
    args = parse_args(argv)
    edition = EDITIONS[args.profile]
    base_url = f"https://www.emqx.com/downloads/{edition}/{args.version}"
    urls = UrlBuilder(base_url, args.profile, args.version)
    matrix = build_matrix.matrix()
    plugins = plugin_rows(REPO_ROOT)

    if args.format == "text":
        print(emit_text(matrix, plugins, urls))
    else:
        print(emit_markdown(matrix, plugins, urls))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
