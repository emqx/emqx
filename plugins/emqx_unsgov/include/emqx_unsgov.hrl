-ifndef(EMQX_UNS_GOV_HRL).
-define(EMQX_UNS_GOV_HRL, true).

-define(DEFAULT_ON_MISMATCH, deny).
-define(DEFAULT_VALIDATE_PAYLOAD, true).
-define(DEFAULT_EXEMPT_TOPICS, []).

-define(MODEL_TAB, emqx_unsgov_model).
-define(DB_SHARD, emqx_unsgov_shard).
%% Unified plugin logging macro with fixed tag/domain.
-include_lib("emqx/include/logger.hrl").
-define(LOG(Level, Data),
    ?SLOG(Level, maps:merge(#{tag => "UNS_GOV", domain => [unsgov]}, (Data)))
).

-record(?MODEL_TAB, {
    id,
    model = #{},
    summary = #{},
    active = true,
    updated_at_ms = 0,
    extra = #{}
}).

-endif.
