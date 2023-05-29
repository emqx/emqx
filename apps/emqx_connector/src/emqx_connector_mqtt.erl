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

-behaviour(emqx_resource).

-export([on_message_received/3]).

%% callbacks of behaviour emqx_resource
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_query_async/4,
    on_get_status/2
]).

-export([on_async_result/2]).

-define(HEALTH_CHECK_TIMEOUT, 1000).

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
    BasicOpts = mk_worker_opts(ResourceId, Conf),
    BridgeOpts = BasicOpts#{
        ingress => mk_ingress_config(maps:get(ingress, Conf, #{}), Conf, ResourceId),
        egress => maps:get(egress, Conf, #{})
    },
    {ok, ClientOpts, WorkerConf} = emqx_connector_mqtt_worker:init(ResourceId, BridgeOpts),
    case emqx_resource_pool:start(ResourceId, emqx_connector_mqtt_worker, ClientOpts) of
        ok ->
            {ok, #{config => WorkerConf}};
        {error, {start_pool_failed, _, Reason}} ->
            {error, Reason}
    end.

on_stop(ResourceId, #{}) ->
    ?SLOG(info, #{
        msg => "stopping_mqtt_connector",
        connector => ResourceId
    }),
    emqx_resource_pool:stop(ResourceId).

on_query(ResourceId, {send_message, Msg}, #{config := Config}) ->
    ?TRACE("QUERY", "send_msg_to_remote_node", #{message => Msg, connector => ResourceId}),
    handle_send_result(with_worker(ResourceId, send_to_remote, [Msg, Config])).

on_query_async(ResourceId, {send_message, Msg}, CallbackIn, #{config := Config}) ->
    ?TRACE("QUERY", "async_send_msg_to_remote_node", #{message => Msg, connector => ResourceId}),
    Callback = {fun on_async_result/2, [CallbackIn]},
    Result = with_worker(ResourceId, send_to_remote_async, [Msg, Callback, Config]),
    case Result of
        ok ->
            ok;
        {ok, Pid} when is_pid(Pid) ->
            {ok, Pid};
        {error, Reason} ->
            {error, classify_error(Reason)}
    end.

with_worker(ResourceId, Fun, Args) ->
    Worker = ecpool:get_client(ResourceId),
    case is_pid(Worker) andalso ecpool_worker:client(Worker) of
        {ok, Client} ->
            erlang:apply(emqx_connector_mqtt_worker, Fun, [Client | Args]);
        {error, Reason} ->
            {error, Reason};
        false ->
            {error, disconnected}
    end.

on_async_result(Callback, Result) ->
    apply_callback_function(Callback, handle_send_result(Result)).

apply_callback_function(F, Result) when is_function(F) ->
    erlang:apply(F, [Result]);
apply_callback_function({F, A}, Result) when is_function(F), is_list(A) ->
    erlang:apply(F, A ++ [Result]);
apply_callback_function({M, F, A}, Result) when is_atom(M), is_atom(F), is_list(A) ->
    erlang:apply(M, F, A ++ [Result]).

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

on_get_status(ResourceId, #{}) ->
    Workers = [Worker || {_Name, Worker} <- ecpool:workers(ResourceId)],
    try emqx_utils:pmap(fun get_status/1, Workers, ?HEALTH_CHECK_TIMEOUT) of
        Statuses ->
            combine_status(Statuses)
    catch
        exit:timeout ->
            connecting
    end.

get_status(Worker) ->
    case ecpool_worker:client(Worker) of
        {ok, Client} ->
            emqx_connector_mqtt_worker:status(Client);
        {error, _} ->
            disconnected
    end.

combine_status(Statuses) ->
    %% NOTE
    %% Natural order of statuses: [connected, connecting, disconnected]
    %% * `disconnected` wins over any other status
    %% * `connecting` wins over `connected`
    case lists:reverse(lists:usort(Statuses)) of
        [Status | _] ->
            Status;
        [] ->
            disconnected
    end.

mk_ingress_config(Ingress, _Conf, _) when map_size(Ingress) == 0 ->
    Ingress;
mk_ingress_config(Ingress, #{hookpoint := HookPoint}, ResourceId) ->
    MFA = {?MODULE, on_message_received, [HookPoint, ResourceId]},
    Ingress#{on_message_received => MFA};
mk_ingress_config(_Ingress, Conf, ResourceId) ->
    error({no_hookpoint_provided, ResourceId, Conf}).

mk_worker_opts(
    ResourceId,
    #{
        server := Server,
        pool_size := PoolSize,
        proto_ver := ProtoVer,
        bridge_mode := BridgeMode,
        clean_start := CleanStart,
        keepalive := KeepAlive,
        retry_interval := RetryIntv,
        max_inflight := MaxInflight,
        ssl := #{enable := EnableSsl} = Ssl
    } = Conf
) ->
    Options = #{
        server => Server,
        pool_size => PoolSize,
        %% 30s
        connect_timeout => 30,
        proto_ver => ProtoVer,
        %% Opening a connection in bridge mode will form a non-standard mqtt connection message.
        %% A load balancing server (such as haproxy) is often set up before the emqx broker server.
        %% When the load balancing server enables mqtt connection packet inspection,
        %% non-standard mqtt connection packets might be filtered out by LB.
        clientid => clientid(ResourceId, Conf),
        bridge_mode => BridgeMode,
        keepalive => ms_to_s(KeepAlive),
        clean_start => CleanStart,
        retry_interval => RetryIntv,
        max_inflight => MaxInflight,
        ssl => EnableSsl,
        ssl_opts => maps:to_list(maps:remove(enable, Ssl))
    },
    maps:merge(
        Options,
        maps:with([username, password], Conf)
    ).

ms_to_s(Ms) ->
    erlang:ceil(Ms / 1000).

clientid(Id, _Conf = #{clientid_prefix := Prefix}) when is_binary(Prefix) ->
    iolist_to_binary([Prefix, ":", Id, ":", atom_to_list(node())]);
clientid(Id, _Conf) ->
    iolist_to_binary([Id, ":", atom_to_list(node())]).
