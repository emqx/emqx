-ifndef(EMQX_RSF_HRL).
-define(EMQX_RSF_HRL, true).

-include_lib("emqx/include/logger.hrl").

-define(PLUGIN_NAME, <<"emqx_rsf">>).
-define(PROGRAM_TAB, emqx_rsf_program).
-define(DB_SHARD, emqx_rsf_shard).
-define(DEFAULT_RUNTIME, <<"python3">>).
-define(MAX_SOURCE_BYTES, 65536).
%% `\z`, not `$`: PCRE `$` also matches before a trailing newline.
-define(NAME_RE, "^[A-Za-z0-9_-][A-Za-z0-9._-]{0,63}\\z").

-define(LOG(Level, Data),
    ?SLOG(Level, maps:merge(#{tag => "RSF", domain => [rsf]}, (Data)))
).

-record(?PROGRAM_TAB, {
    name :: binary(),
    runtime :: binary(),
    source :: binary(),
    sha256 :: binary(),
    updated_at_ms :: integer(),
    updated_by :: binary(),
    extra = #{} :: map()
}).

-endif.
