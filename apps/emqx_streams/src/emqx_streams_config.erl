%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_config).

-include("emqx_streams_internal.hrl").

-export([is_enabled/0, max_stream_count/0]).

-export([
    pre_config_update/3,
    post_config_update/5
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec is_enabled() -> boolean().
is_enabled() ->
    emqx:get_config([?SCHEMA_ROOT, enable]).

-spec max_stream_count() -> pos_integer().
max_stream_count() ->
    emqx:get_config([?SCHEMA_ROOT, max_stream_count]).

%%------------------------------------------------------------------------------
%% Config hooks
%%------------------------------------------------------------------------------

pre_config_update([?SCHEMA_ROOT], NewConf, _OldConf) ->
    {ok, NewConf}.

post_config_update([?SCHEMA_ROOT], _Request, _NewConf, _OldConf, _AppEnvs) ->
    % ok ?= maybe_enable(NewConf, OldConf)
    ok.
