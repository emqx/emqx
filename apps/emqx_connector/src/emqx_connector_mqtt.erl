%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(emqx_connector_mqtt).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").

-behaviour(supervisor).
-behaviour(emqx_resource).

%% API and callbacks for supervisor
-export([
    callback_mode/0,
    start_link/0,
    init/1,
    create_bridge/1,
    drop_bridge/1,
    bridges/0
]).

-export([on_message_received/3]).

%% callbacks of behaviour emqx_resource
-export([
    on_start/2,
    on_stop/2,
    on_query/3,
    on_query_async/4,
    on_get_status/2
]).

-behaviour(hocon_schema).

-import(hoconsc, [mk/2]).

-export([
    roots/0,
    fields/1
]).

%%=====================================================================
%% Hocon schema
roots() ->
    fields("config").

fields("config") ->
    emqx_connector_mqtt_schema:fields("config");
fields("get") ->
    [
        {num_of_bridges,
            mk(
                integer(),
                #{desc => ?DESC("num_of_bridges")}
            )}
    ] ++ fields("post");
fields("put") ->
    emqx_connector_mqtt_schema:fields("server_configs");
fields("post") ->
    [
        {type,
            mk(
                mqtt,
                #{
                    required => true,
                    desc => ?DESC("type")
                }
            )},
        {name,
            mk(
                binary(),
                #{
                    required => true,
                    desc => ?DESC("name")
                }
            )}
    ] ++ fields("put").

%% ===================================================================
%% supervisor APIs
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlag = #{
        strategy => one_for_one,
        intensity => 100,
        period => 10
    },
    {ok, {SupFlag, []}}.

bridge_spec(Config) ->
    #{
        id => maps:get(name, Config),
        start => {emqx_connector_mqtt_worker, start_link, [Config]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [emqx_connector_mqtt_worker]
    }.

-spec bridges() -> [{node(), map()}].
bridges() ->
    [
        {Name, emqx_connector_mqtt_worker:status(Name)}
     || {Name, _Pid, _, _} <- supervisor:which_children(?MODULE)
    ].

create_bridge(Config) ->
    supervisor:start_child(?MODULE, bridge_spec(Config)).

drop_bridge(Name) ->
    case supervisor:terminate_child(?MODULE, Name) of
        ok ->
            supervisor:delete_child(?MODULE, Name);
        {error, not_found} ->
            ok;
        {error, Error} ->
            {error, Error}
    end.

%% ===================================================================
%% When use this bridge as a data source, ?MODULE:on_message_received will be called
%% if the bridge received msgs from the remote broker.
on_message_received(Msg, HookPoint, ResId) ->
    emqx_resource:inc_received(ResId),
    emqx:run_hook(HookPoint, [Msg]).

%% ===================================================================
callback_mode() -> async_if_possible.

on_start(InstId, Conf) ->
    InstanceId = binary_to_atom(InstId, utf8),
    ?SLOG(info, #{
        msg => "starting_mqtt_connector",
        connector => InstanceId,
        config => Conf
    }),
    BasicConf = basic_config(Conf),
    BridgeConf = BasicConf#{
        name => InstanceId,
        clientid => clientid(InstId),
        subscriptions => make_sub_confs(maps:get(ingress, Conf, undefined), Conf, InstId),
        forwards => make_forward_confs(maps:get(egress, Conf, undefined))
    },
    case ?MODULE:create_bridge(BridgeConf) of
        {ok, _Pid} ->
            ensure_mqtt_worker_started(InstanceId, BridgeConf);
        {error, {already_started, _Pid}} ->
            ok = ?MODULE:drop_bridge(InstanceId),
            {ok, _} = ?MODULE:create_bridge(BridgeConf),
            ensure_mqtt_worker_started(InstanceId, BridgeConf);
        {error, Reason} ->
            {error, Reason}
    end.

on_stop(_InstId, #{name := InstanceId}) ->
    ?SLOG(info, #{
        msg => "stopping_mqtt_connector",
        connector => InstanceId
    }),
    case ?MODULE:drop_bridge(InstanceId) of
        ok ->
            ok;
        {error, not_found} ->
            ok;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "stop_mqtt_connector",
                connector => InstanceId,
                reason => Reason
            })
    end.

on_query(_InstId, {send_message, Msg}, #{name := InstanceId}) ->
    ?TRACE("QUERY", "send_msg_to_remote_node", #{message => Msg, connector => InstanceId}),
    emqx_connector_mqtt_worker:send_to_remote(InstanceId, Msg).

on_query_async(
    _InstId,
    {send_message, Msg},
    {ReplayFun, Args},
    #{name := InstanceId}
) ->
    ?TRACE("QUERY", "async_send_msg_to_remote_node", #{message => Msg, connector => InstanceId}),
    emqx_connector_mqtt_worker:send_to_remote_async(InstanceId, Msg, {ReplayFun, Args}).

on_get_status(_InstId, #{name := InstanceId, bridge_conf := Conf}) ->
    AutoReconn = maps:get(auto_reconnect, Conf, true),
    case emqx_connector_mqtt_worker:status(InstanceId) of
        connected -> connected;
        _ when AutoReconn == true -> connecting;
        _ when AutoReconn == false -> disconnected
    end.

ensure_mqtt_worker_started(InstanceId, BridgeConf) ->
    case emqx_connector_mqtt_worker:ensure_started(InstanceId) of
        ok -> {ok, #{name => InstanceId, bridge_conf => BridgeConf}};
        {error, Reason} -> {error, Reason}
    end.

make_sub_confs(EmptyMap, _Conf, _) when map_size(EmptyMap) == 0 ->
    undefined;
make_sub_confs(undefined, _Conf, _) ->
    undefined;
make_sub_confs(SubRemoteConf, Conf, InstId) ->
    ResId = emqx_resource_manager:manager_id_to_resource_id(InstId),
    case maps:find(hookpoint, Conf) of
        error ->
            error({no_hookpoint_provided, Conf});
        {ok, HookPoint} ->
            MFA = {?MODULE, on_message_received, [HookPoint, ResId]},
            SubRemoteConf#{on_message_received => MFA}
    end.

make_forward_confs(EmptyMap) when map_size(EmptyMap) == 0 ->
    undefined;
make_forward_confs(undefined) ->
    undefined;
make_forward_confs(FrowardConf) ->
    FrowardConf.

basic_config(
    #{
        server := Server,
        reconnect_interval := ReconnIntv,
        proto_ver := ProtoVer,
        bridge_mode := BridgeMode,
        clean_start := CleanStart,
        keepalive := KeepAlive,
        retry_interval := RetryIntv,
        max_inflight := MaxInflight,
        ssl := #{enable := EnableSsl} = Ssl
    } = Conf
) ->
    BaiscConf = #{
        %% connection opts
        server => Server,
        %% 30s
        connect_timeout => 30,
        auto_reconnect => true,
        reconnect_interval => ReconnIntv,
        proto_ver => ProtoVer,
        %% Opening bridge_mode will form a non-standard mqtt connection message.
        %% A load balancing server (such as haproxy) is often set up before the emqx broker server.
        %% When the load balancing server enables mqtt connection packet inspection,
        %% non-standard mqtt connection packets will be filtered out by LB.
        %% So let's disable bridge_mode.
        bridge_mode => BridgeMode,
        clean_start => CleanStart,
        keepalive => ms_to_s(KeepAlive),
        retry_interval => RetryIntv,
        max_inflight => MaxInflight,
        ssl => EnableSsl,
        ssl_opts => maps:to_list(maps:remove(enable, Ssl)),
        if_record_metrics => true
    },
    maybe_put_fields([username, password], Conf, BaiscConf).

maybe_put_fields(Fields, Conf, Acc0) ->
    lists:foldl(
        fun(Key, Acc) ->
            case maps:find(Key, Conf) of
                error -> Acc;
                {ok, Val} -> Acc#{Key => Val}
            end
        end,
        Acc0,
        Fields
    ).

ms_to_s(Ms) ->
    erlang:ceil(Ms / 1000).

clientid(Id) ->
    iolist_to_binary([Id, ":", atom_to_list(node())]).
