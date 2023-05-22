%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").

-behaviour(supervisor).
-behaviour(emqx_resource).

%% API and callbacks for supervisor
-export([
    callback_mode/0,
    start_link/0,
    init/1,
    create_bridge/2,
    remove_bridge/1,
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

-export([on_async_result/2]).

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

bridge_spec(Name, Options) ->
    #{
        id => Name,
        start => {emqx_connector_mqtt_worker, start_link, [Name, Options]},
        restart => temporary,
        shutdown => 1000
    }.

-spec bridges() -> [{_Name, _Status}].
bridges() ->
    [
        {Name, emqx_connector_mqtt_worker:status(Name)}
     || {Name, _Pid, _, _} <- supervisor:which_children(?MODULE)
    ].

create_bridge(Name, Options) ->
    supervisor:start_child(?MODULE, bridge_spec(Name, Options)).

remove_bridge(Name) ->
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
    emqx_resource_metrics:received_inc(ResId),
    emqx:run_hook(HookPoint, [Msg]).

%% ===================================================================
callback_mode() -> async_if_possible.

on_start(ResourceId, Conf) ->
    ?SLOG(info, #{
        msg => "starting_mqtt_connector",
        connector => ResourceId,
        config => emqx_utils:redact(Conf)
    }),
    BasicConf = basic_config(Conf),
    BridgeOpts = BasicConf#{
        clientid => clientid(ResourceId, Conf),
        subscriptions => make_sub_confs(maps:get(ingress, Conf, #{}), Conf, ResourceId),
        forwards => maps:get(egress, Conf, #{})
    },
    case create_bridge(ResourceId, BridgeOpts) of
        {ok, Pid, {ConnProps, WorkerConf}} ->
            {ok, #{
                name => ResourceId,
                worker => Pid,
                config => WorkerConf,
                props => ConnProps
            }};
        {error, {already_started, _Pid}} ->
            ok = remove_bridge(ResourceId),
            on_start(ResourceId, Conf);
        {error, Reason} ->
            {error, Reason}
    end.

on_stop(ResourceId, #{}) ->
    ?SLOG(info, #{
        msg => "stopping_mqtt_connector",
        connector => ResourceId
    }),
    case remove_bridge(ResourceId) of
        ok ->
            ok;
        {error, not_found} ->
            ok;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "stop_mqtt_connector_error",
                connector => ResourceId,
                reason => Reason
            })
    end.

on_query(ResourceId, {send_message, Msg}, #{worker := Pid, config := Config}) ->
    ?TRACE("QUERY", "send_msg_to_remote_node", #{message => Msg, connector => ResourceId}),
    Result = emqx_connector_mqtt_worker:send_to_remote(Pid, Msg, Config),
    handle_send_result(Result).

on_query_async(ResourceId, {send_message, Msg}, CallbackIn, #{worker := Pid, config := Config}) ->
    ?TRACE("QUERY", "async_send_msg_to_remote_node", #{message => Msg, connector => ResourceId}),
    Callback = {fun on_async_result/2, [CallbackIn]},
    case emqx_connector_mqtt_worker:send_to_remote_async(Pid, Msg, Callback, Config) of
        ok ->
            ok;
        {ok, Pid} ->
            {ok, Pid};
        {error, _} = Error ->
            handle_send_result(Error)
    end.

on_async_result(Callback, Result) ->
    apply_callback_function(Callback, handle_send_result(Result)).

apply_callback_function(F, Result) when is_function(F) ->
    erlang:apply(F, [Result]);
apply_callback_function({F, A}, Result) when is_function(F), is_list(A) ->
    erlang:apply(F, A ++ [Result]);
apply_callback_function({M, F, A}, Result) when is_atom(M), is_atom(F), is_list(A) ->
    erlang:apply(M, F, A ++ [Result]).

on_get_status(_ResourceId, #{worker := Pid}) ->
    emqx_connector_mqtt_worker:status(Pid).

handle_send_result(ok) ->
    ok;
handle_send_result({ok, #{reason_code := ?RC_SUCCESS}}) ->
    ok;
handle_send_result({ok, #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS}}) ->
    ok;
handle_send_result({ok, Reply}) ->
    {error, classify_reply(Reply)};
handle_send_result({error, Reason}) ->
    {error, classify_error(Reason)}.

classify_reply(Reply = #{reason_code := _}) ->
    {unrecoverable_error, Reply}.

classify_error(disconnected = Reason) ->
    {recoverable_error, Reason};
classify_error({disconnected, _RC, _} = Reason) ->
    {recoverable_error, Reason};
classify_error({shutdown, _} = Reason) ->
    {recoverable_error, Reason};
classify_error(shutdown = Reason) ->
    {recoverable_error, Reason};
classify_error(Reason) ->
    {unrecoverable_error, Reason}.

make_sub_confs(Subscriptions, _Conf, _) when map_size(Subscriptions) == 0 ->
    Subscriptions;
make_sub_confs(Subscriptions, #{hookpoint := HookPoint}, ResourceId) ->
    MFA = {?MODULE, on_message_received, [HookPoint, ResourceId]},
    Subscriptions#{on_message_received => MFA};
make_sub_confs(_SubRemoteConf, Conf, ResourceId) ->
    error({no_hookpoint_provided, ResourceId, Conf}).

basic_config(
    #{
        server := Server,
        proto_ver := ProtoVer,
        bridge_mode := BridgeMode,
        clean_start := CleanStart,
        keepalive := KeepAlive,
        retry_interval := RetryIntv,
        max_inflight := MaxInflight,
        ssl := #{enable := EnableSsl} = Ssl
    } = Conf
) ->
    BasicConf = #{
        server => Server,
        %% 30s
        connect_timeout => 30,
        proto_ver => ProtoVer,
        %% Opening a connection in bridge mode will form a non-standard mqtt connection message.
        %% A load balancing server (such as haproxy) is often set up before the emqx broker server.
        %% When the load balancing server enables mqtt connection packet inspection,
        %% non-standard mqtt connection packets might be filtered out by LB.
        bridge_mode => BridgeMode,
        keepalive => ms_to_s(KeepAlive),
        clean_start => CleanStart,
        retry_interval => RetryIntv,
        max_inflight => MaxInflight,
        ssl => EnableSsl,
        ssl_opts => maps:to_list(maps:remove(enable, Ssl))
    },
    maps:merge(
        BasicConf,
        maps:with([username, password], Conf)
    ).

ms_to_s(Ms) ->
    erlang:ceil(Ms / 1000).

clientid(Id, _Conf = #{clientid_prefix := Prefix}) when is_binary(Prefix) ->
    iolist_to_binary([Prefix, ":", Id, ":", atom_to_list(node())]);
clientid(Id, _Conf) ->
    iolist_to_binary([Id, ":", atom_to_list(node())]).
