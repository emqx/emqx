# Translations maintained in this repository

`rel/i18n` holds the English descriptions, which are the source of truth.
Translations normally live in [emqx/emqx-i18n](https://github.com/emqx/emqx-i18n)
and are downloaded by `scripts/pre-compile.sh`; this directory is for the ones
kept here instead.

| File | Language tag | Source |
| --- | --- | --- |
| `desc.zh-TW.hocon` | `zh-TW` | generated from `desc.zh.hocon` by `gen-zh-TW.py` |

`scripts/pre-compile.sh` copies each file here to
`apps/emqx_dashboard/priv/desc.<lang>.hocon`, where
`emqx_dashboard_desc_cache` picks the language tag up from the file name. The
tag is also the value `dashboard.i18n_lang` accepts, and the lookup is a case
sensitive exact match, so the two must be spelled identically.

## Regenerating the Traditional Chinese file

```sh
pip install opencc-python-reimplemented
./scripts/pre-compile.sh                 # fetches the current desc.zh.hocon
./rel/i18n-tr/gen-zh-TW.py               # rewrites desc.zh-TW.hocon
git diff rel/i18n-tr/desc.zh-TW.hocon    # review before committing
```

Conversion is OpenCC's `s2twp`, then the term overrides in `gen-zh-TW.py`,
which fix the words OpenCC gets wrong for Taiwanese usage (`引數` -> `參數`,
`全域性` -> `全域`, `聯結器` -> `連接器` and so on) and the ones Taiwan simply
words differently (`訪問` -> `存取`, `證書` -> `憑證`). `CORRECTIONS` in the
same file replaces individual entries whose Simplified text is stale or wrong.

Keep the term table in step with
`scripts/i18n/zh-TW-terms.json` in
[emqx/emqx-dashboard5](https://github.com/emqx/emqx-dashboard5), so the
Dashboard UI and these descriptions use the same wording.

## Checking the result

```sh
./rel/i18n-tr/lint-zh-TW.py
```

`zh-TW-lint.json` lists the wording that must not appear: terms that are
Mainland rather than Taiwanese, the forms OpenCC produces that Taiwan does not
use, and terms that are only wrong outside the contexts a rule allows — `協議`
is right in `授權協議` and wrong everywhere else, `型別` is right in front of a
primitive type name and `類型` everywhere else, and `供應商` belongs to the AI
providers while everything else is a `提供者`. The script also reports
Simplified characters left behind, and any space between two Han characters
that the Simplified source does not have, which is how a term rule that
replaces an English word usually goes wrong.

`scripts/i18n/lint-zh-TW.mjs` in
[emqx/emqx-dashboard5](https://github.com/emqx/emqx-dashboard5) runs the same
rules against the Dashboard strings. Keep the two copies of `zh-TW-lint.json`
identical, as with the term table.

## Fallback

`emqx_dashboard_desc_cache:lookup/5` falls back to `en` when a text is missing,
so `zh-TW` is a locale in its own right rather than a patch on top of `zh`: an
entry it does not carry shows the English text, never the Simplified Chinese
one.
