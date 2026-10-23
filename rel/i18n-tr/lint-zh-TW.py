#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Check rel/i18n-tr/desc.zh-TW.hocon for wording that does not belong in it.

    ./rel/i18n-tr/lint-zh-TW.py

Three checks:

  1. the terms in zh-TW-lint.json — Mainland wording, the forms OpenCC produces
     that Taiwan does not use, and terms that are only wrong outside the
     contexts a rule allows;
  2. Simplified characters left behind, from the list in zh-TW-lint.json
     (OpenCC widens the net when it happens to be installed, but is not
     required);
  3. spacing the conversion introduced — a space between two Han characters
     that the Simplified source does not have. Needs that source, which
     scripts/pre-compile.sh leaves in apps/emqx_dashboard/priv/desc.zh.hocon;
     the check is skipped when it is not there, because the source has plenty
     of such spacing of its own and reporting it would be noise.

Exits non-zero when anything is found, so it can run in CI beside
scripts/check-i18n-style.sh.
"""
import argparse
import io
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
LINT_FILE = os.path.join(HERE, "zh-TW-lint.json")
DEFAULT_TARGET = os.path.join(HERE, "desc.zh-TW.hocon")
DEFAULT_SOURCE = os.path.join(ROOT, "apps", "emqx_dashboard", "priv", "desc.zh.hocon")

# OpenCC's s2t normalises these to an orthodox variant, but the form s2twp
# produces is the one Taiwan writes, so they are not leftovers.
TW_VARIANTS = set("台峰游秘里群")

HAN_SPACE = re.compile(r"[一-鿿] +[一-鿿]")


def load(path):
    out = {}
    for line in io.open(path, encoding="utf-8"):
        m = re.match(r'^([^\s=]+)\s*=\s*"(.*)"\s*$', line)
        if m:
            out[m.group(1)] = m.group(2)
    return out


def show(text, needle, width=34):
    flat = text.replace("\\n", " ")
    i = flat.find(needle)
    if i < 0:
        return flat[: width * 2]
    return flat[max(0, i - width) : i + len(needle) + width]


def check_terms(entries, rules):
    found = []
    for key, text in entries.items():
        for rule in rules:
            if any(key.startswith(p) for p in rule.get("allowKeyPrefix", ())):
                continue
            probe = text
            for allowed in rule.get("allow", ()):
                probe = probe.replace(allowed, "")
            if rule["term"] in probe:
                found.append((rule, key, text))
    return found


def check_simplified(entries, simplified_chars):
    """Uses the character list in zh-TW-lint.json, so this needs no OpenCC.

    With OpenCC installed the net is wider: it also catches a Simplified
    character the Simplified source did not happen to use before.
    """
    charset = set(simplified_chars)
    convert = None
    try:
        import opencc
        convert = opencc.OpenCC("s2t").convert
    except ImportError:
        pass
    found = []
    for key, text in entries.items():
        bad = {c for c in text if c in charset}
        if convert is not None:
            bad |= {c for c in text
                    if "一" <= c <= "鿿"
                    and c not in TW_VARIANTS and convert(c) != c}
        if bad:
            found.append((key, "".join(sorted(bad)), text))
    return found


def check_spacing(entries, source):
    if source is None:
        return None
    found = []
    for key, text in entries.items():
        was = len(HAN_SPACE.findall(source.get(key, "")))
        now = HAN_SPACE.findall(text)
        if len(now) > was:
            found.append((key, now[0], text))
    return found


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("target", nargs="?", default=DEFAULT_TARGET)
    ap.add_argument("--source", default=DEFAULT_SOURCE,
                    help="the Simplified Chinese file the target was generated from")
    args = ap.parse_args()

    lint = json.loads(io.open(LINT_FILE, encoding="utf-8").read())
    rules = lint["rules"]
    entries = load(args.target)
    source = load(args.source) if os.path.isfile(args.source) else None
    print("checking %d entries in %s against %d rules"
          % (len(entries), os.path.basename(args.target), len(rules)))

    problems = 0

    found = check_terms(entries, rules)
    if found:
        problems += len(found)
        print("\n%d entries use wording that should not appear:" % len(found))
        for rule, key, text in found[:40]:
            note = "   # " + rule["note"] if rule.get("note") else ""
            print("   %s" % key)
            print("      %s -> %s%s" % (rule["term"], rule["prefer"], note))
            print("      ...%s..." % show(text, rule["term"]))
        if len(found) > 40:
            print("   ... and %d more" % (len(found) - 40))

    simplified = check_simplified(entries, lint.get("simplifiedChars", ""))
    if simplified:
        problems += len(simplified)
        print("\n%d entries still hold Simplified characters:" % len(simplified))
        for key, chars, text in simplified[:20]:
            print("   %-52s %s" % (key, chars))
            print("      ...%s..." % show(text, chars[0]))

    spacing = check_spacing(entries, source)
    if spacing is None:
        print("\nskipped the spacing check (%s not found; run scripts/pre-compile.sh)"
              % os.path.relpath(args.source, ROOT))
    elif spacing:
        problems += len(spacing)
        print("\n%d entries gained a space between two Han characters:" % len(spacing))
        for key, frag, text in spacing[:20]:
            print("   %-52s %r" % (key, frag))
            print("      ...%s..." % show(text, frag))

    if problems:
        print("\n%d problems found" % problems)
        return 1
    print("\nOK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
