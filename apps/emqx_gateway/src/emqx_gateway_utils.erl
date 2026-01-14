%%--------------------------------------------------------------------
%% Copyright (c) 2021-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gateway_utils).

-moduledoc """
Utility functions for EMQX gateway.
""".

-include("emqx_gateway.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(GATEWAY_APP_MODULES, [
    emqx_gateway_coap,
    emqx_gateway_exproto,
    emqx_gateway_gbt32960,
    emqx_gateway_jt808,
    emqx_gateway_lwm2m,
    emqx_gateway_mqttsn,
    emqx_gateway_ocpp,
    emqx_gateway_stomp,
    emqx_gateway_nats
]).

-export([
    childspec/2,
    childspec/3,
    childspec/4,
    supervisor_ret/1,
    find_sup_child/2
]).

-export([
    start_listeners/1,
    stop_listeners/1,
    is_listener_running/1,
    update_listeners/1,
    update_gateway_listeners/3
]).

-export([
    apply/2,
    parse_listenon/1,
    unix_ts_to_rfc3339/2,
    listener_id/3,
    listener_name_from_id/1,
    parse_listener_id/1,
    protocol/1,
    global_chain/1,
    listener_chain/3,
    find_gateway_definitions/0,
    find_gateway_definition/1,
    add_max_connections/2,
    random_clientid/1
]).

-export([stringfy/1]).

%% Common Envs
-export([
    active_n/1,
    ratelimit/1,
    frame_options/1,
    init_gc_state/1,
    stats_timer/1,
    idle_timeout/1,
    oom_policy/1
]).

-export([
    default_subopts/0
]).

-type gw_name() :: emqx_gateway_utils_conf:gw_name().
-type listener_runtime_config() :: emqx_gateway_utils_conf:listener_runtime_config().
-type listener_runtime_id() :: emqx_gateway_utils_conf:listener_runtime_id().

-define(ACTIVE_N, 10).
-define(DEFAULT_IDLE_TIMEOUT, 30000).
-define(DEFAULT_GC_OPTS, #{count => 1000, bytes => 1024 * 1024}).
-define(DEFAULT_OOM_POLICY, #{
    max_heap_size => 4194304,
    max_mailbox_size => 32000
}).

-elvis([{elvis_style, god_modules, disable}]).

-spec childspec(worker | supervisor, Mod :: atom()) ->
    supervisor:child_spec().
childspec(Type, Mod) ->
    childspec(Mod, Type, Mod, []).

-spec childspec(worker | supervisor, Mod :: atom(), Args :: list()) ->
    supervisor:child_spec().
childspec(Type, Mod, Args) ->
    childspec(Mod, Type, Mod, Args).

-spec childspec(atom(), worker | supervisor, Mod :: atom(), Args :: list()) ->
    supervisor:child_spec().
childspec(Id, Type, Mod, Args) ->
    #{
        id => Id,
        start => {Mod, start_link, Args},
        type => Type
    }.

-spec supervisor_ret(supervisor:startchild_ret()) ->
    {ok, pid()}
    | {error, supervisor:startchild_err()}.
supervisor_ret({ok, Pid, _Info}) ->
    {ok, Pid};
supervisor_ret({error, {Reason, Child}}) ->
    case element(1, Child) == child of
        true -> {error, Reason};
        _ -> {error, {Reason, Child}}
    end;
supervisor_ret(Ret) ->
    Ret.

-spec find_sup_child(Sup :: pid() | atom(), ChildId :: term()) ->
    false
    | {ok, pid()}.
find_sup_child(Sup, ChildId) ->
    case lists:keyfind(ChildId, 1, supervisor:which_children(Sup)) of
        false -> false;
        {_Id, Pid, _Type, _Mods} -> {ok, Pid}
    end.

-doc """
Start listeners of a gateway using runtime listener configurations.
""".
-spec start_listeners(list(listener_runtime_config())) ->
    {ok, [pid()]}
    | {error, {_Reason :: term(), listener_runtime_config()}}.
start_listeners(ListenerConfigs) ->
    start_listeners(ListenerConfigs, []).

start_listeners([], Acc) ->
    {ok, [Pid || {_ListenerConfig, Pid} <- Acc]};
start_listeners([ListenerConfig | ListenerConfigs], Acc) ->
    case start_listener(ListenerConfig) of
        {ok, Pid} ->
            NAcc = Acc ++ [{ListenerConfig, Pid}],
            start_listeners(ListenerConfigs, NAcc);
        {error, Reason} ->
            %% Rollback all started listeners
            lists:foreach(
                fun({StartedListenerConfig, _Pid}) ->
                    stop_listener(StartedListenerConfig)
                end,
                Acc
            ),
            {error, {Reason, ListenerConfig}}
    end.

-doc """
Start a single listener.
""".
-spec start_listener(listener_runtime_config()) -> {ok, pid()} | {error, term()}.
start_listener(ListenerConfig) ->
    case do_start_listener(ListenerConfig) of
        {ok, Pid} ->
            ?tp(debug, gateway_listener_started, #{
                listener_config => ListenerConfig,
                pid => Pid
            }),
            {ok, Pid};
        {error, {already_started, Pid}} ->
            ?tp(debug, gateway_listener_already_started, #{
                listener_config => ListenerConfig,
                pid => Pid
            }),
            {ok, Pid};
        {error, Reason} ->
            ?tp(debug, gateway_listener_start_failed, #{
                listener_config => ListenerConfig,
                reason => Reason
            }),
            emqx_gateway_utils:supervisor_ret({error, Reason})
    end.

do_start_listener(#{
    listener_id := ListenerId,
    listen_on := ListenOn,
    listener_opts := {esockd, #{type := Type, socket_opts := SocketOpts, mfa := MFA}}
}) when
    Type == tcp orelse Type == ssl
->
    esockd:open(ListenerId, ListenOn, SocketOpts, MFA);
%
do_start_listener(#{
    listener_id := ListenerId,
    listen_on := ListenOn,
    listener_opts := {esockd, #{type := udp, socket_opts := SocketOpts, mfa := MFA}}
}) ->
    esockd:open_udp(ListenerId, ListenOn, SocketOpts, MFA);
%
do_start_listener(#{
    listener_id := ListenerId,
    listen_on := ListenOn,
    listener_opts := {esockd, #{type := dtls, socket_opts := SocketOpts, mfa := MFA}}
}) ->
    esockd:open_dtls(ListenerId, ListenOn, SocketOpts, MFA);
%
do_start_listener(#{
    listener_id := ListenerId,
    listener_opts := {cowboy, #{type := ws, ranch_opts := RanchOpts, ws_opts := WsOpts}}
}) ->
    cowboy:start_clear(ListenerId, RanchOpts, WsOpts);
%
do_start_listener(#{
    listener_id := ListenerId,
    listener_opts := {cowboy, #{type := wss, ranch_opts := RanchOpts, ws_opts := WsOpts}}
}) ->
    cowboy:start_tls(ListenerId, RanchOpts, WsOpts).

-doc """
Stop specified listeners of a gateway using runtime listener configurations or runtime listener ids.
""".
-spec stop_listeners(list(listener_runtime_config() | listener_runtime_id())) -> ok.
stop_listeners(ListenerConfigs) ->
    lists:foreach(fun stop_listener/1, ListenerConfigs).

-doc """
Stop a single listener of a gateway.
""".
-spec stop_listener(listener_runtime_config() | listener_runtime_id()) -> ok.
stop_listener(#{listener_opts := _} = ListenerConfig) ->
    stop_listener(emqx_gateway_utils_conf:rt_listener_id(ListenerConfig));
stop_listener(#{listener_id := ListenerId, listen_on := ListenOn} = ListenerRuntimeId) ->
    StopRet = do_stop_listener(ListenerRuntimeId),
    case StopRet of
        ok ->
            ?tp(debug, gateway_listener_stopped, #{
                listener => ListenerRuntimeId
            });
        {error, Reason} ->
            ListenOnStr = emqx_listeners:format_bind(ListenOn),
            ?tp(error, gateway_listener_stop_failed, #{
                listener => ListenerRuntimeId,
                reason => Reason
            }),
            ?ELOG(
                "Failed to stop gateway ~p on ~ts: ~0p~n",
                [ListenerId, ListenOnStr, Reason]
            )
    end,
    StopRet.

do_stop_listener(#{listener_id := ListenerId, listen_on := ListenOn, listener_type := {esockd, _}}) ->
    esockd:close(ListenerId, ListenOn);
do_stop_listener(#{listener_id := ListenerId, listen_on := ListenOn, listener_type := {cowboy, _}}) ->
    case cowboy:stop_listener(ListenerId) of
        ok ->
            wait_cowboy_listener_stopped(ListenOn);
        Error ->
            Error
    end.

-spec is_listener_running(listener_runtime_id()) -> boolean().
is_listener_running(#{
    listener_id := ListenerId, listen_on := ListenOn, listener_type := {esockd, _}
}) ->
    try esockd:listener({ListenerId, ListenOn}) of
        Pid when is_pid(Pid) ->
            true
    catch
        _:_ ->
            false
    end;
is_listener_running(#{listener_id := ListenerId, listener_type := {cowboy, _}}) ->
    try ranch:get_status(ListenerId) of
        running ->
            true;
        _ ->
            false
    catch
        _:_ ->
            false
    end.

wait_cowboy_listener_stopped(ListenOn) ->
    emqx_listeners:wait_cowboy_listener_stopped(ListenOn).

-spec update_gateway_listeners(
    gw_name(), list(listener_runtime_config()), list(listener_runtime_config())
) ->
    {ok, [pid()]} | {error, term()}.
update_gateway_listeners(GwName, OldListenerConfigs, NewListenerConfigs) ->
    try
        Diff = emqx_gateway_utils_conf:diff_rt_listener_configs(
            OldListenerConfigs, NewListenerConfigs
        ),
        case emqx_gateway_utils:update_listeners(Diff) of
            {ok, NewPids} ->
                {ok, NewPids};
            {error, Reason} ->
                ?SLOG(error, #{
                    msg => "gateway_update_failed",
                    reason => Reason,
                    gateway => GwName,
                    diff => Diff
                }),
                {error, Reason}
        end
    catch
        Class:Error:Stk ->
            ?SLOG(error, #{
                msg => "gateway_update_failed",
                class => Class,
                reason => Error,
                stacktrace => Stk,
                gateway => GwName
            }),
            {error, Error}
    end.

-doc """
Update listeners of a gateway.
In case of error, the newly started listeners will be stopped.
However, updated listeners will not be rolled back.
""".
-spec update_listeners(#{
    stop := list(listener_runtime_config()),
    update := list(listener_runtime_config()),
    start := list(listener_runtime_config())
}) ->
    {ok, [pid()]} | {error, term()}.
update_listeners(#{
    stop := StopConfigs,
    update := UpdateConfigs,
    start := StartConfigs
}) ->
    ok = stop_listeners(StopConfigs),
    ok = lists:foreach(fun update_listener/1, UpdateConfigs),
    case start_listeners(StartConfigs) of
        {ok, AddPids} ->
            {ok, AddPids};
        {error, Reason} ->
            {error, Reason}
    end.

update_listener(#{
    listener_id := ListenerId,
    listen_on := ListenOn,
    listener_opts := {esockd, #{socket_opts := SocketOpts, mfa := MFA}}
}) ->
    NewOptions = [{connection_mfargs, MFA} | SocketOpts],
    esockd:set_options({ListenerId, ListenOn}, NewOptions);
update_listener(#{
    listener_id := ListenerId,
    listen_on := ListenOn,
    listener_opts := {cowboy, #{ranch_opts := RanchOpts, ws_opts := WsOpts}}
}) ->
    ok = ranch:suspend_listener(ListenerId),
    ok = ranch:set_transport_options(ListenerId, RanchOpts),
    ok = ranch:set_protocol_options(ListenerId, WsOpts),
    %% NOTE: ranch:suspend_listener/1 will close the listening socket,
    %% so we need to wait for the listener to be stopped.
    ok = wait_cowboy_listener_stopped(ListenOn),
    ranch:resume_listener(ListenerId).

apply({M, F, A}, A2) when
    is_atom(M),
    is_atom(M),
    is_list(A),
    is_list(A2)
->
    erlang:apply(M, F, A ++ A2);
apply({F, A}, A2) when
    is_function(F),
    is_list(A),
    is_list(A2)
->
    erlang:apply(F, A ++ A2);
apply(F, A2) when
    is_function(F),
    is_list(A2)
->
    erlang:apply(F, A2).

parse_listenon(Port) when is_integer(Port) ->
    Port;
parse_listenon(IpPort) when is_tuple(IpPort) ->
    IpPort;
parse_listenon(Str) when is_binary(Str) ->
    parse_listenon(binary_to_list(Str));
parse_listenon(Str) when is_list(Str) ->
    try
        list_to_integer(Str)
    catch
        _:_ ->
            case emqx_schema:to_ip_port(Str) of
                {ok, R} -> R;
                {error, _} -> error({invalid_listenon_name, Str})
            end
    end.

listener_id(GwName, Type, LisName) ->
    binary_to_atom(
        <<(bin(GwName))/binary, ":", (bin(Type))/binary, ":", (bin(LisName))/binary>>
    ).

listener_name_from_id(ListenerId) ->
    {_GwName, _Type, Name} = parse_listener_id(ListenerId),
    binary_to_existing_atom(Name).

parse_listener_id(Id) when is_atom(Id) ->
    parse_listener_id(atom_to_binary(Id));
parse_listener_id(Id) ->
    try
        [GwName, Type, Name] = binary:split(bin(Id), <<":">>, [global]),
        {GwName, Type, Name}
    catch
        _:_ -> error({invalid_listener_id, Id})
    end.

-spec protocol(gw_name()) -> atom().
protocol(stomp) -> stomp;
protocol(mqttsn) -> 'mqtt-sn';
protocol(coap) -> coap;
protocol(lwm2m) -> lwm2m;
protocol(exproto) -> exproto;
protocol(jt808) -> jt808;
protocol(gbt32960) -> gbt32960;
protocol(ocpp) -> ocpp;
protocol(nats) -> nats;
protocol(GwName) -> error({invalid_protocol_name, GwName}).

-spec global_chain(gw_name()) -> atom().
global_chain(GwName) ->
    emqx_authn_chains:global_chain(protocol(GwName)).

listener_chain(GwName, Type, LisName) ->
    listener_id(GwName, Type, LisName).

bin(A) when is_atom(A) ->
    atom_to_binary(A);
bin(L) when is_list(L); is_binary(L) ->
    iolist_to_binary(L).

unix_ts_to_rfc3339(Keys, Map) when is_list(Keys) ->
    lists:foldl(fun(K, Acc) -> unix_ts_to_rfc3339(K, Acc) end, Map, Keys);
unix_ts_to_rfc3339(Key, Map) ->
    case maps:get(Key, Map, undefined) of
        undefined ->
            Map;
        Ts ->
            Map#{
                Key => emqx_utils_calendar:epoch_to_rfc3339(Ts)
            }
    end.

-spec stringfy(term()) -> binary().
stringfy(T) when is_list(T); is_binary(T) ->
    iolist_to_binary(T);
stringfy(T) ->
    iolist_to_binary(io_lib:format("~0p", [T])).

%%--------------------------------------------------------------------
%% Envs

active_n(Options) ->
    maps:get(active_n, Options, ?ACTIVE_N).

-spec idle_timeout(map()) -> pos_integer().
idle_timeout(Options) ->
    maps:get(idle_timeout, Options, ?DEFAULT_IDLE_TIMEOUT).

-spec ratelimit(map()) -> esockd_rate_limit:config() | undefined.
ratelimit(Options) ->
    maps:get(ratelimit, Options, undefined).

-spec frame_options(map()) -> map().
frame_options(Options) ->
    maps:get(frame, Options, #{}).

-spec init_gc_state(map()) -> emqx_gc:gc_state() | undefined.
init_gc_state(Options) ->
    emqx_utils:maybe_apply(fun emqx_gc:init/1, force_gc_policy(Options)).

-spec force_gc_policy(map()) -> emqx_gc:opts() | undefined.
force_gc_policy(Options) ->
    maps:get(force_gc_policy, Options, ?DEFAULT_GC_OPTS).

-spec oom_policy(map()) -> emqx_types:oom_policy().
oom_policy(Options) ->
    maps:get(force_shutdown_policy, Options, ?DEFAULT_OOM_POLICY).

-spec stats_timer(map()) -> undefined | disabled.
stats_timer(Options) ->
    case enable_stats(Options) of
        true -> undefined;
        false -> disabled
    end.

-spec enable_stats(map()) -> boolean().
enable_stats(Options) ->
    maps:get(enable_stats, Options, true).

%%--------------------------------------------------------------------
%% Envs2

default_subopts() ->
    %% Retain Handling
    #{
        rh => 1,
        %% Retain as Publish
        rap => 0,
        %% No Local
        nl => 0,
        %% QoS
        qos => 0,
        is_new => true
    }.

-spec find_gateway_definitions() -> list(gateway_def()).
find_gateway_definitions() ->
    case persistent_term:get(emqx_gateways, no_value) of
        no_value ->
            Definitions = do_find_gateway_definitions(),
            _ = persistent_term:put(emqx_gateways, {value, Definitions}),
            Definitions;
        {value, Definitions} ->
            Definitions
    end.

do_find_gateway_definitions() ->
    lists:flatmap(
        fun(AppModule) -> find_gateway_attrs(AppModule) end,
        ?GATEWAY_APP_MODULES
    ).

-spec find_gateway_definition(atom()) -> {ok, map()} | {error, term()}.
find_gateway_definition(Name) ->
    case
        lists:search(
            fun(#{name := GwName}) ->
                GwName =:= Name
            end,
            find_gateway_definitions()
        )
    of
        {value, Definition} ->
            {ok, Definition};
        false ->
            {error, not_found}
    end.

find_gateway_attrs(AppMod) ->
    [
        Attr
     || {gateway, Attrs} <- module_attributes(AppMod),
        Attr <- Attrs
    ].

module_attributes(Module) ->
    try
        apply(Module, module_info, [attributes])
    catch
        error:undef -> []
    end.

-spec add_max_connections(non_neg_integer() | infinity, non_neg_integer() | infinity) ->
    pos_integer() | infinity.
add_max_connections(_, infinity) ->
    infinity;
add_max_connections(infinity, _) ->
    infinity;
add_max_connections(A, B) when is_integer(A) andalso is_integer(B) ->
    A + B.

random_clientid(GwName) when is_atom(GwName) ->
    iolist_to_binary([atom_to_list(GwName), "-", emqx_utils:gen_id()]).
