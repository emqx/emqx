# -*- mode: yaml; -*-
otp_path: "~/.asdf/installs/erlang/24.3.4.2-3/"
apps_dirs:
  - "apps/*"
  - "lib-ce/*"
  - "lib-ee/*"
deps_dirs:{% for dir in default_deps_dirs %}
  - "{{dir}}"{% endfor %}
  - "_build/test/lib/bbmustache"
  - "_build/test/lib/meck"
  - "_build/test/lib/proper"
  - "_build/test/lib/erl_csv"
include_dirs:
  - "apps/"
  - "apps/*/include"
  - "_build/default/lib/typerefl/include"
  - "_build/default/lib/"
  - "_build/default/lib/*/include"
  - "_build/test/lib/bbmustache"
  - "_build/test/lib/meck"
  - "_build/test/lib/proper"
exclude_unused_includes:
  - "typerefl/include/types.hrl"
  - "logger.hrl"
diagnostics:
  enabled:
    - bound_var_in_pattern
    - elvis
    - unused_includes
    - unused_macros
    - crossref
    # - dialyzer
    - compiler
  disabled:
    - dialyzer
    # - crossref
    # - compiler
lenses:
  disabled:
    # - show-behaviour-usages
    # - ct-run-test
    - server-info
  enable:
    - show-behaviour-usages
    - ct-run-test
macros:
  - name: EMQX_RELEASE_EDITION
    value: ee
plt_path:
  - emqx_dialyzer_24.3.4.2-2_plt
code_reload:
  node: emqx@127.0.0.1
