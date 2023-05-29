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
-module(emqx_bridge_mqtt_connector).

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
    case start_ingress(ResourceId, Conf) of
        {ok, Result1} ->
            case start_egress(ResourceId, Conf) of
                {ok, Result2} ->
                    {ok, maps:merge(Result1, Result2)};
                {error, Reason} ->
                    _ = stop_ingress(Result1),
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

start_ingress(ResourceId, Conf) ->
    ClientOpts = mk_client_opts(ResourceId, "ingress", Conf),
    case mk_ingress_config(ResourceId, Conf) of
        Ingress = #{} ->
            start_ingress(ResourceId, Ingress, ClientOpts);
        undefined ->
            {ok, #{}}
    end.

start_ingress(ResourceId, Ingress, ClientOpts) ->
    PoolName = <<ResourceId/binary, ":ingress">>,
    PoolSize = choose_ingress_pool_size(Ingress),
    Options = [
        {name, PoolName},
        {pool_size, PoolSize},
        {ingress, Ingress},
        {client_opts, ClientOpts}
    ],
    case emqx_resource_pool:start(PoolName, emqx_bridge_mqtt_ingress, Options) of
        ok ->
            {ok, #{ingress_pool_name => PoolName}};
        {error, {start_pool_failed, _, Reason}} ->
            {error, Reason}
    end.

choose_ingress_pool_size(#{remote := #{topic := RemoteTopic}, pool_size := PoolSize}) ->
    case emqx_topic:parse(RemoteTopic) of
        {_Filter, #{share := _Name}} ->
            % NOTE: this is shared subscription, many workers may subscribe
            PoolSize;
        {_Filter, #{}} ->
            % NOTE: this is regular subscription, only one worker should subscribe
            ?SLOG(warning, #{
                msg => "ingress_pool_size_ignored",
                reason =>
                    "Remote topic filter is not a shared subscription, "
                    "ingress pool will start with a single worker",
                config_pool_size => PoolSize,
                pool_size => 1
            }),
            1
    end.

start_egress(ResourceId, Conf) ->
    % NOTE
    % We are ignoring the user configuration here because there's currently no reliable way
    % to ensure proper session recovery according to the MQTT spec.
    ClientOpts = maps:put(clean_start, true, mk_client_opts(ResourceId, "egress", Conf)),
    case mk_egress_config(Conf) of
        Egress = #{} ->
            start_egress(ResourceId, Egress, ClientOpts);
        undefined ->
            {ok, #{}}
    end.

start_egress(ResourceId, Egress, ClientOpts) ->
    PoolName = <<ResourceId/binary, ":egress">>,
    PoolSize = maps:get(pool_size, Egress),
    Options = [
        {name, PoolName},
        {pool_size, PoolSize},
        {client_opts, ClientOpts}
    ],
    case emqx_resource_pool:start(PoolName, emqx_bridge_mqtt_egress, Options) of
        ok ->
            {ok, #{
                egress_pool_name => PoolName,
                egress_config => emqx_bridge_mqtt_egress:config(Egress)
            }};
        {error, {start_pool_failed, _, Reason}} ->
            {error, Reason}
    end.

on_stop(ResourceId, State) ->
    ?SLOG(info, #{
        msg => "stopping_mqtt_connector",
        connector => ResourceId
    }),
    ok = stop_ingress(State),
    ok = stop_egress(State).

stop_ingress(#{ingress_pool_name := PoolName}) ->
    emqx_resource_pool:stop(PoolName);
stop_ingress(#{}) ->
    ok.

stop_egress(#{egress_pool_name := PoolName}) ->
    emqx_resource_pool:stop(PoolName);
stop_egress(#{}) ->
    ok.

on_query(
    ResourceId,
    {send_message, Msg},
    #{egress_pool_name := PoolName, egress_config := Config}
) ->
    ?TRACE("QUERY", "send_msg_to_remote_node", #{message => Msg, connector => ResourceId}),
    handle_send_result(with_egress_client(PoolName, send, [Msg, Config]));
on_query(ResourceId, {send_message, Msg}, #{}) ->
    ?SLOG(error, #{
        msg => "forwarding_unavailable",
        connector => ResourceId,
        message => Msg,
        reason => "Egress is not configured"
    }).

on_query_async(
    ResourceId,
    {send_message, Msg},
    CallbackIn,
    #{egress_pool_name := PoolName, egress_config := Config}
) ->
    ?TRACE("QUERY", "async_send_msg_to_remote_node", #{message => Msg, connector => ResourceId}),
    Callback = {fun on_async_result/2, [CallbackIn]},
    Result = with_egress_client(PoolName, send_async, [Msg, Callback, Config]),
    case Result of
        ok ->
            ok;
        {ok, Pid} when is_pid(Pid) ->
            {ok, Pid};
        {error, Reason} ->
            {error, classify_error(Reason)}
    end;
on_query_async(ResourceId, {send_message, Msg}, _Callback, #{}) ->
    ?SLOG(error, #{
        msg => "forwarding_unavailable",
        connector => ResourceId,
        message => Msg,
        reason => "Egress is not configured"
    }).

with_egress_client(ResourceId, Fun, Args) ->
    ecpool:pick_and_do(ResourceId, {emqx_bridge_mqtt_egress, Fun, Args}, no_handover).

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
classify_error(ecpool_empty) ->
    {recoverable_error, disconnected};
classify_error({disconnected, _RC, _} = Reason) ->
    {recoverable_error, Reason};
classify_error({shutdown, _} = Reason) ->
    {recoverable_error, Reason};
classify_error(shutdown = Reason) ->
    {recoverable_error, Reason};
classify_error(Reason) ->
    {unrecoverable_error, Reason}.

on_get_status(_ResourceId, State) ->
    Pools = maps:to_list(maps:with([ingress_pool_name, egress_pool_name], State)),
    Workers = [{Pool, Worker} || {Pool, PN} <- Pools, {_Name, Worker} <- ecpool:workers(PN)],
    try emqx_utils:pmap(fun get_status/1, Workers, ?HEALTH_CHECK_TIMEOUT) of
        Statuses ->
            combine_status(Statuses)
    catch
        exit:timeout ->
            connecting
    end.

get_status({Pool, Worker}) ->
    case ecpool_worker:client(Worker) of
        {ok, Client} when Pool == ingress_pool_name ->
            emqx_bridge_mqtt_ingress:status(Client);
        {ok, Client} when Pool == egress_pool_name ->
            emqx_bridge_mqtt_egress:status(Client);
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

mk_ingress_config(
    ResourceId,
    #{
        ingress := Ingress = #{remote := _},
        server := Server,
        hookpoint := HookPoint
    }
) ->
    Ingress#{
        server => Server,
        on_message_received => {?MODULE, on_message_received, [HookPoint, ResourceId]}
    };
mk_ingress_config(ResourceId, #{ingress := #{remote := _}} = Conf) ->
    error({no_hookpoint_provided, ResourceId, Conf});
mk_ingress_config(_ResourceId, #{}) ->
    undefined.

mk_egress_config(#{egress := Egress = #{remote := _}}) ->
    Egress;
mk_egress_config(#{}) ->
    undefined.

mk_client_opts(
    ResourceId,
    ClientScope,
    Config = #{
        server := Server,
        keepalive := KeepAlive,
        ssl := #{enable := EnableSsl} = Ssl
    }
) ->
    HostPort = emqx_bridge_mqtt_connector_schema:parse_server(Server),
    Options = maps:with(
        [
            proto_ver,
            username,
            password,
            clean_start,
            retry_interval,
            max_inflight,
            % Opening a connection in bridge mode will form a non-standard mqtt connection message.
            % A load balancing server (such as haproxy) is often set up before the emqx broker server.
            % When the load balancing server enables mqtt connection packet inspection,
            % non-standard mqtt connection packets might be filtered out by LB.
            bridge_mode
        ],
        Config
    ),
    Options#{
        hosts => [HostPort],
        clientid => clientid(ResourceId, ClientScope, Config),
        connect_timeout => 30,
        keepalive => ms_to_s(KeepAlive),
        force_ping => true,
        ssl => EnableSsl,
        ssl_opts => maps:to_list(maps:remove(enable, Ssl))
    }.

ms_to_s(Ms) ->
    erlang:ceil(Ms / 1000).

clientid(Id, ClientScope, _Conf = #{clientid_prefix := Prefix}) when is_binary(Prefix) ->
    iolist_to_binary([Prefix, ":", Id, ":", ClientScope, ":", atom_to_list(node())]);
clientid(Id, ClientScope, _Conf) ->
    iolist_to_binary([Id, ":", ClientScope, ":", atom_to_list(node())]).
