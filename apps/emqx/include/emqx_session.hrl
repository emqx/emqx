%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_SESSION_HRL).
-define(EMQX_SESSION_HRL, true).

-define(IS_SESSION_IMPL_MEM(S), (is_tuple(S) andalso element(1, S) =:= session)).
-define(IS_SESSION_IMPL_DS(S), (is_map_key(id, S))).

%% (Erlang) messages that a connection process should forward to the
%% session handler.
-define(session_message(MSG), MSG).

%% The session attributes cached in the `emqx_channel_info` table for every session
%% implementation. Each implementation's `chan_info/1` adds its own ones and `impl`.
%%
%% The counters and gauges live in the stats array of the same table row (see
%% `emqx_chan_stats`). The attributes here do not change with traffic. `subscriptions` is
%% left out because building that map costs O(number of subscriptions) on every refresh.
%% `emqx_session:info/1` stays complete: it is the payload of the `session.created` and
%% `session.resumed` hooks.
-define(CHAN_INFO_SESSION_KEYS, [
    created_at,
    is_persistent,
    durable,
    subscriptions_max,
    awaiting_rel_max,
    inflight_max
]).

-endif.
