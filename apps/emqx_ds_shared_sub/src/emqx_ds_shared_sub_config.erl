%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_config).

-behaviour(emqx_config_handler).
-behaviour(emqx_config_backup).

-type update_request() :: emqx_config:config().

%% callbacks for emqx_config_handler
-export([
    pre_config_update/3,
    post_config_update/5
]).

%% callbacks for emqx_config_backup
-export([
    import_config/1
]).

%% API
-export([
    load/0,
    unload/0,
    get/1
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec load() -> ok.
load() ->
    emqx_conf:add_handler([durable_queues], ?MODULE).

-spec unload() -> ok.
unload() ->
    ok = emqx_conf:remove_handler([durable_queues]).

-spec get(atom() | [atom()]) -> term().
get(Name) when is_atom(Name) ->
    emqx_config:get([durable_queues, Name]);
get(Name) when is_list(Name) ->
    emqx_config:get([durable_queues | Name]).

%%--------------------------------------------------------------------
%% emqx_config_handler callbacks
%%--------------------------------------------------------------------

-spec pre_config_update(list(atom()), update_request(), emqx_config:raw_config()) ->
    {ok, emqx_config:update_request()}.
pre_config_update([durable_queues | _], NewConfig, _OldConfig) ->
    {ok, NewConfig}.

-spec post_config_update(
    list(atom()),
    update_request(),
    emqx_config:config(),
    emqx_config:config(),
    emqx_config:app_envs()
) ->
    ok.
post_config_update([durable_queues | _], _Req, _NewConfig, _OldConfig, _AppEnvs) ->
    ok.

%%----------------------------------------------------------------------------------------
%% Data backup
%%----------------------------------------------------------------------------------------

import_config(#{<<"durable_queues">> := DQConf}) ->
    OldDQConf = emqx:get_raw_config([durable_queues], #{}),
    NewDQConf = maps:merge(OldDQConf, DQConf),
    case emqx_conf:update([durable_queues], NewDQConf, #{override_to => cluster}) of
        {ok, #{raw_config := NewRawConf}} ->
            Changed = maps:get(changed, emqx_utils_maps:diff_maps(NewRawConf, DQConf)),
            ChangedPaths = [[durable_queues, K] || K <- maps:keys(Changed)],
            {ok, #{root_key => durable_queues, changed => ChangedPaths}};
        Error ->
            {error, #{root_key => durable_queues, reason => Error}}
    end;
import_config(_) ->
    {ok, #{root_key => durable_queues, changed => []}}.
