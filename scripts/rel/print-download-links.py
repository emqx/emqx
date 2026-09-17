#!/usr/bin/env python3
"""Print the download URLs for every package produced for a release.

Release notes link to https://www.emqx.com/downloads/... instead of attaching
the packages to the GitHub release, so download statistics stay visible.

This script is pure enumeration: it never touches the network. It reads the
checked-out tree -- the release version (./pkg-vsn.sh <profile> --release), the
platform matrix (scripts/rel/build_matrix.py), and the plugin versions -- and
expands them into URLs, so it can run at any point in the release cycle without
secrets. It uses only the Python standard library.

Two families of URLs are printed:
  - EMQX packages, served from the emqx.com download CDN under
    /downloads/<edition>/<version>/ (edition: enterprise or broker).
  - Plugin packages (emqx-enterprise only), under
    /downloads/emqx-plugins/e<version>/. Each plugin under plugins/ ships a
    VERSION file that gives its package version. Release lines without a
    plugins/ directory print no plugin links.

With --s3, the URLs point at the public S3 bucket the release workflow uploads
to (see .github/workflows/build_packages.yaml) instead of the CDN. These serve
the same objects, and are useful to check what was published before a CDN
cache invalidation lands.

v5 naming notes:
  - Release tags carry a prefix: e<version> for emqx-enterprise and
    v<version> for emqx. S3 directories are named after the tag. The CDN uses
    the bare version for packages and the tag for the plugin directory.
  - The linux matrix also builds one Elixir release
    (<profile>-<version>-elixir-ubuntu22.04-amd64.tar.gz). It is not an offered
    download, so it is omitted here.

Output is a plain list of URLs; --md prints markdown tables instead.

Usage:
  print-download-links.py [version] [--md] [--profile <profile>] [--s3]

The version defaults to ./pkg-vsn.sh <profile> --release when omitted. The tag
form (e5.8.12, v5.8.9) is accepted too.

Examples:
  print-download-links.py
  print-download-links.py 5.8.12 --md
  print-download-links.py e5.8.12 --s3
  print-download-links.py 5.8.9 --profile emqx --md
"""

import argparse
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_matrix  # noqa: E402 (sibling module, path set above)

REPO_ROOT = Path(__file__).resolve().parents[2]

CDN_BASE_URL = "https://www.emqx.com/downloads"
S3_BASE_URL = "https://packages.emqx.io"
PLUGINS_DIR = "emqx-plugins"

# Per profile: git tag prefix, CDN edition directory, S3 top-level directory,
# and whether plugin packages are published. The S3 directories mirror the
# `aws s3 cp` targets in build_packages.yaml and the `s3dir` output in
# release.yaml.
PROFILES = {
    "emqx-enterprise": {
        "tag_prefix": "e",
        "cdn_dir": "enterprise",
        "s3_dir": "emqx-ee",
        "plugins": True,
    },
    "emqx": {
        "tag_prefix": "v",
        "cdn_dir": "broker",
        "s3_dir": "emqx-ce",
        "plugins": False,
    },
}


def linux_pkg_ext(os_token):
    """Native package extension for a linux os token.

    Mirrors the PKGERDIR logic in scripts/buildx.sh / build.
    """
    if os_token.startswith(("ubuntu", "debian", "raspbian")):
        return "deb"
    return "rpm"


def release_version(profile):
    """The release version of the checked-out tree, via ./pkg-vsn.sh <profile> --release."""
    return subprocess.run(
        [str(REPO_ROOT / "pkg-vsn.sh"), profile, "--release"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def parse_args(argv):
    parser = argparse.ArgumentParser(
        add_help=True,
        usage="%(prog)s [version] [--md] [--profile <profile>] [--s3]",
    )
    parser.add_argument(
        "version_pos",
        nargs="?",
        metavar="version",
        help="release version; defaults to ./pkg-vsn.sh <profile> --release",
    )
    parser.add_argument("--version", dest="version_opt")
    parser.add_argument(
        "--md",
        action="store_true",
        help="print markdown tables instead of a plain URL list",
    )
    parser.add_argument(
        "--profile", default="emqx-enterprise", choices=list(PROFILES)
    )
    parser.add_argument(
        "--s3",
        action="store_true",
        help="link to the public S3 bucket instead of the CDN",
    )
    args = parser.parse_args(argv)

    version = args.version_opt or args.version_pos or release_version(args.profile)
    # Accept the tag form (e5.8.12, v5.8.9) as well as the bare version.
    prefix = PROFILES[args.profile]["tag_prefix"]
    if version.startswith(prefix) and version[len(prefix):][:1].isdigit():
        version = version[len(prefix):]
    args.version = version
    return args


def plugin_rows(repo_root):
    """(name, version) for every plugin under plugins/ that ships a VERSION file.

    Sorted by plugin name for stable output. Empty when plugins/ does not exist.
    """
    plugins_dir = repo_root / "plugins"
    rows = []
    for version_file in sorted(plugins_dir.glob("*/VERSION")):
        name = version_file.parent.name
        version = version_file.read_text().strip()
        rows.append((name, version))
    return rows


class UrlBuilder:
    def __init__(self, base_url, plugins_base_url, profile, version):
        self.base_url = base_url
        self.plugins_base_url = plugins_base_url
        self.profile = profile
        self.version = version

    def pkg_url(self, os_token, arch, ext):
        # <os>-<arch>.<ext> style (linux/mac)
        return (
            f"{self.base_url}/{self.profile}-{self.version}-"
            f"{os_token}-{arch}.{ext}"
        )

    def plugin_url(self, name, plugin_version):
        # <name>-<plugin_version>.tar.gz under the release-tag directory.
        return f"{self.plugins_base_url}/{name}-{plugin_version}.tar.gz"

    def plugin_sha256_url(self, name, plugin_version):
        # The plugin build writes <name>-<vsn>.sha256, replacing the extension,
        # whereas `build' writes <package>.sha256, appending to it.
        return f"{self.plugins_base_url}/{name}-{plugin_version}.sha256"


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


def md_table(headers, rows):
    """A GitHub-flavoured markdown table. Returns [] when there are no rows."""
    if not rows:
        return []
    sep = ["---"] * len(headers)
    out = ["| " + " | ".join(headers) + " |", "| " + " | ".join(sep) + " |"]
    out += ["| " + " | ".join(cells) + " |" for cells in rows]
    return out


def emit_markdown(matrix, plugins, urls):
    lines = ["## Download", ""]

    def pkg_links(os_token, arch, ext):
        """Link to a package, followed by its .sha256 sidecar.

        `build' writes <package>.sha256 next to every package it produces, and
        both are published together.
        """
        url = urls.pkg_url(os_token, arch, ext)
        return f"{md_link(f'.{ext}', url)} ({md_link('sha256', url + '.sha256')})"

    def linux_row(row):
        ext = linux_pkg_ext(row["os"])
        return [
            f"`{row['os']}`",
            row["arch"],
            pkg_links(row["os"], row["arch"], ext),
            pkg_links(row["os"], row["arch"], "tar.gz"),
        ]

    def linux_section(title, prefixes):
        rows = [
            linux_row(row)
            for row in matrix["linux"]
            if row["os"].startswith(prefixes)
        ]
        if not rows:
            return
        lines.append(f"### {title}")
        lines.append("")
        lines.extend(md_table(["OS", "Arch", "Package", "Tarball"], rows))
        lines.append("")

    linux_section("Ubuntu / Debian", ("ubuntu", "debian"))
    linux_section("RHEL / Rocky / Amazon Linux", ("el", "amzn"))

    mac_rows = [
        [f"`{row['os']}`", row["arch"], pkg_links(row["os"], row["arch"], "zip")]
        for row in matrix["mac"]
    ]
    if mac_rows:
        lines.append("### macOS")
        lines.append("")
        lines.extend(md_table(["OS", "Arch", "Package"], mac_rows))
        lines.append("")

    if plugins:
        lines.append("### Plugins")
        lines.append("")
        # Plugin packages are always .tar.gz, so the ext is implied.
        plugin_rows_md = [
            [
                f"`{name}`",
                version,
                f"{md_link('.tar.gz', urls.plugin_url(name, version))} "
                f"({md_link('sha256', urls.plugin_sha256_url(name, version))})",
            ]
            for name, version in plugins
        ]
        lines.extend(md_table(["Plugin", "Version", "Package"], plugin_rows_md))
        lines.append("")

    return "\n".join(lines).rstrip("\n")


def main(argv):
    args = parse_args(argv)
    conf = PROFILES[args.profile]
    tag = f"{conf['tag_prefix']}{args.version}"
    if args.s3:
        base_url = f"{S3_BASE_URL}/{conf['s3_dir']}/{tag}"
        plugins_base_url = f"{S3_BASE_URL}/{PLUGINS_DIR}/{tag}"
    else:
        base_url = f"{CDN_BASE_URL}/{conf['cdn_dir']}/{args.version}"
        plugins_base_url = f"{CDN_BASE_URL}/{PLUGINS_DIR}/{tag}"
    urls = UrlBuilder(base_url, plugins_base_url, args.profile, args.version)
    plugins = plugin_rows(REPO_ROOT) if conf["plugins"] else []

    matrix = build_matrix.matrix()
    if args.md:
        print(emit_markdown(matrix, plugins, urls))
    else:
        print(emit_text(matrix, plugins, urls))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
