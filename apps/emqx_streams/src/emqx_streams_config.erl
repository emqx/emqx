%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_config).

-export([is_enabled/0]).

-export([
    pre_config_update/3,
    post_config_update/5
]).

-define(CONFIG_ROOT, durable_streams).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec is_enabled() -> boolean().
is_enabled() ->
    emqx:get_config([?CONFIG_ROOT, enable]).

%%------------------------------------------------------------------------------
%% Config hooks
%%------------------------------------------------------------------------------

pre_config_update([?CONFIG_ROOT], NewConf, _OldConf) ->
    {ok, NewConf}.

post_config_update([?CONFIG_ROOT], _Request, _NewConf, _OldConf, _AppEnvs) ->
    % ok ?= maybe_enable(NewConf, OldConf)
    ok.
