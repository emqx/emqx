-ifndef(EMQX_UNS_GATE_HRL).
-define(EMQX_UNS_GATE_HRL, true).

-define(DEFAULT_ENABLED, true).
-define(DEFAULT_ON_MISMATCH, deny).
-define(DEFAULT_ALLOW_INTERMEDIATE_PUBLISH, false).
-define(DEFAULT_VALIDATE_PAYLOAD, true).
-define(DEFAULT_EXEMPT_TOPICS, [<<"$SYS/#">>, <<"$share/#">>]).

-endif.
