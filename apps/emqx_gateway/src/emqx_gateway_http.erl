%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Gateway Interface Module for HTTP-APIs
-module(emqx_gateway_http).

-include("include/emqx_gateway.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_authentication.hrl").

-define(AUTHN, ?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_ATOM).

-import(emqx_gateway_utils, [listener_id/3]).

%% Mgmt APIs - gateway
-export([gateways/1]).

%% Mgmt APIs
-export([
    add_listener/2,
    remove_listener/1,
    update_listener/2
]).

-export([
    authn/1,
    authn/2,
    add_authn/2,
    add_authn/3,
    update_authn/2,
    update_authn/3,
    remove_authn/1,
    remove_authn/2
]).

%% Mgmt APIs - clients
-export([
    lookup_client/3,
    kickout_client/2,
    list_client_subscriptions/2,
    client_subscribe/4,
    client_unsubscribe/3
]).

%% Utils for http, swagger, etc.
-export([
    return_http_error/2,
    with_gateway/2,
    with_authn/2,
    with_listener_authn/3,
    checks/2,
    reason2resp/1,
    reason2msg/1,
    sum_cluster_connections/1
]).

%% RPC
-export([gateway_status/1, cluster_gateway_status/1]).

-type gateway_summary() ::
    #{
        name := binary(),
        status := running | stopped | unloaded,
        created_at => binary(),
        started_at => binary(),
        stopped_at => binary(),
        max_connections => integer(),
        current_connections => integer(),
        listeners => []
    }.

-elvis([
    {elvis_style, god_modules, disable},
    {elvis_style, no_nested_try_catch, disable},
    {elvis_style, invalid_dynamic_call, disable}
]).

-define(DEFAULT_CALL_TIMEOUT, 15000).

%%--------------------------------------------------------------------
%% Mgmt APIs - gateway
%%--------------------------------------------------------------------

-spec gateways(Status :: all | running | stopped | unloaded) ->
    [gateway_summary()].
gateways(Status) ->
    Gateways = lists:map(
        fun({GwName, _}) ->
            case emqx_gateway:lookup(GwName) of
                undefined ->
                    #{name => GwName, status => unloaded};
                GwInfo = #{config := Config} ->
                    GwInfo0 = emqx_gateway_utils:unix_ts_to_rfc3339(
                        [created_at, started_at, stopped_at],
                        GwInfo
                    ),
                    GwInfo1 = maps:with(
                        [
                            name,
                            status,
                            created_at,
                            started_at,
                            stopped_at
                        ],
                        GwInfo0
                    ),
                    NodeStatus = cluster_gateway_status(GwName),
                    {MaxCons, CurrCons} = sum_cluster_connections(NodeStatus),
                    GwInfo1#{
                        max_connections => MaxCons,
                        current_connections => CurrCons,
                        listeners => get_listeners_status(GwName, Config),
                        node_status => NodeStatus
                    }
            end
        end,
        emqx_gateway_registry:list()
    ),
    case Status of
        all -> Gateways;
        _ -> [Gw || Gw = #{status := S} <- Gateways, S == Status]
    end.

gateway_status(GwName) ->
    case emqx_gateway:lookup(GwName) of
        undefined ->
            #{node => node(), status => unloaded};
        #{status := Status, config := Config} ->
            #{
                node => node(),
                status => Status,
                max_connections => max_connections_count(Config),
                current_connections => current_connections_count(GwName)
            }
    end.

cluster_gateway_status(GwName) ->
    Nodes = mria_mnesia:running_nodes(),
    case emqx_gateway_http_proto_v1:get_cluster_status(Nodes, GwName) of
        {Results, []} ->
            Results;
        {_, _BadNodes} ->
            error(badrpc)
    end.

%% @private
max_connections_count(Config) ->
    Listeners = emqx_gateway_utils:normalize_config(Config),
    lists:foldl(
        fun({_, _, _, SocketOpts, _}, Acc) ->
            Acc + proplists:get_value(max_connections, SocketOpts, 0)
        end,
        0,
        Listeners
    ).

%% @private
current_connections_count(GwName) ->
    try
        InfoTab = emqx_gateway_cm:tabname(info, GwName),
        ets:info(InfoTab, size)
    catch
        _:_ ->
            0
    end.

%% @private
get_listeners_status(GwName, Config) ->
    Listeners = emqx_gateway_utils:normalize_config(Config),
    lists:map(
        fun({Type, LisName, ListenOn, _, _}) ->
            Name0 = listener_id(GwName, Type, LisName),
            Name = {Name0, ListenOn},
            LisO = #{id => Name0, type => Type, name => LisName},
            case catch esockd:listener(Name) of
                _Pid when is_pid(_Pid) ->
                    LisO#{running => true};
                _ ->
                    LisO#{running => false}
            end
        end,
        Listeners
    ).

%%--------------------------------------------------------------------
%% Mgmt APIs - listeners
%%--------------------------------------------------------------------

-spec add_listener(atom() | binary(), map()) -> {ok, map()}.
add_listener(ListenerId, NewConf0) ->
    {GwName, Type, Name} = emqx_gateway_utils:parse_listener_id(ListenerId),
    NewConf = maps:without(
        [
            <<"id">>,
            <<"name">>,
            <<"type">>,
            <<"running">>
        ],
        NewConf0
    ),
    confexp(emqx_gateway_conf:add_listener(GwName, {Type, Name}, NewConf)).

-spec update_listener(atom() | binary(), map()) -> {ok, map()}.
update_listener(ListenerId, NewConf0) ->
    {GwName, Type, Name} = emqx_gateway_utils:parse_listener_id(ListenerId),

    NewConf = maps:without(
        [
            <<"id">>,
            <<"name">>,
            <<"type">>,
            <<"running">>
        ],
        NewConf0
    ),
    confexp(emqx_gateway_conf:update_listener(GwName, {Type, Name}, NewConf)).

-spec remove_listener(binary()) -> ok.
remove_listener(ListenerId) ->
    {GwName, Type, Name} = emqx_gateway_utils:parse_listener_id(ListenerId),
    confexp(emqx_gateway_conf:remove_listener(GwName, {Type, Name})).

-spec authn(gateway_name()) -> map().
authn(GwName) ->
    %% XXX: Need append chain-nanme, authenticator-id?
    Path = [gateway, GwName, ?AUTHN],
    ChainName = emqx_gateway_utils:global_chain(GwName),
    wrap_chain_name(
        ChainName,
        emqx_map_lib:jsonable_map(emqx:get_raw_config(Path))
    ).

-spec authn(gateway_name(), binary()) -> map().
authn(GwName, ListenerId) ->
    {_, Type, Name} = emqx_gateway_utils:parse_listener_id(ListenerId),
    Path = [gateway, GwName, listeners, Type, Name, ?AUTHN],
    ChainName = emqx_gateway_utils:listener_chain(GwName, Type, Name),
    wrap_chain_name(
        ChainName,
        emqx_map_lib:jsonable_map(emqx:get_raw_config(Path))
    ).

wrap_chain_name(ChainName, Conf) ->
    case emqx_authentication:list_authenticators(ChainName) of
        {ok, [#{id := Id} | _]} ->
            Conf#{chain_name => ChainName, id => Id};
        _ ->
            Conf
    end.

-spec add_authn(gateway_name(), map()) -> {ok, map()}.
add_authn(GwName, AuthConf) ->
    confexp(emqx_gateway_conf:add_authn(GwName, AuthConf)).

-spec add_authn(gateway_name(), binary(), map()) -> {ok, map()}.
add_authn(GwName, ListenerId, AuthConf) ->
    {_, LType, LName} = emqx_gateway_utils:parse_listener_id(ListenerId),
    confexp(emqx_gateway_conf:add_authn(GwName, {LType, LName}, AuthConf)).

-spec update_authn(gateway_name(), map()) -> {ok, map()}.
update_authn(GwName, AuthConf) ->
    confexp(emqx_gateway_conf:update_authn(GwName, AuthConf)).

-spec update_authn(gateway_name(), binary(), map()) -> {ok, map()}.
update_authn(GwName, ListenerId, AuthConf) ->
    {_, LType, LName} = emqx_gateway_utils:parse_listener_id(ListenerId),
    confexp(emqx_gateway_conf:update_authn(GwName, {LType, LName}, AuthConf)).

-spec remove_authn(gateway_name()) -> ok.
remove_authn(GwName) ->
    confexp(emqx_gateway_conf:remove_authn(GwName)).

-spec remove_authn(gateway_name(), binary()) -> ok.
remove_authn(GwName, ListenerId) ->
    {_, LType, LName} = emqx_gateway_utils:parse_listener_id(ListenerId),
    confexp(emqx_gateway_conf:remove_authn(GwName, {LType, LName})).

confexp(ok) -> ok;
confexp({ok, Res}) -> {ok, Res};
confexp({error, Reason}) -> error(Reason).

%%--------------------------------------------------------------------
%% Mgmt APIs - clients
%%--------------------------------------------------------------------

-spec lookup_client(
    gateway_name(),
    emqx_types:clientid(),
    {module(), atom()}
) -> list().
lookup_client(GwName, ClientId, {M, F}) ->
    [
        begin
            Info = emqx_gateway_cm:get_chan_info(GwName, ClientId, Pid),
            Stats = emqx_gateway_cm:get_chan_stats(GwName, ClientId, Pid),
            M:F({{ClientId, Pid}, Info, Stats})
        end
     || Pid <- emqx_gateway_cm:lookup_by_clientid(GwName, ClientId)
    ].

-spec kickout_client(gateway_name(), emqx_types:clientid()) ->
    {error, any()}
    | ok.
kickout_client(GwName, ClientId) ->
    Results = [
        emqx_gateway_cm:kick_session(GwName, ClientId, Pid)
     || Pid <- emqx_gateway_cm:lookup_by_clientid(GwName, ClientId)
    ],
    IsOk = lists:any(fun(Item) -> Item =:= ok end, Results),
    case {IsOk, Results} of
        {true, _} -> ok;
        {_, []} -> {error, not_found};
        {false, _} -> lists:last(Results)
    end.

-spec list_client_subscriptions(gateway_name(), emqx_types:clientid()) ->
    {error, any()}
    | {ok, list()}.
list_client_subscriptions(GwName, ClientId) ->
    case client_call(GwName, ClientId, subscriptions) of
        {error, Reason} ->
            {error, Reason};
        {ok, Subs} ->
            {ok,
                lists:map(
                    fun({Topic, SubOpts}) ->
                        SubOpts#{topic => Topic}
                    end,
                    Subs
                )}
    end.

-spec client_subscribe(
    gateway_name(),
    emqx_types:clientid(),
    emqx_types:topic(),
    emqx_types:subopts()
) ->
    {error, any()}
    | {ok, {emqx_types:topic(), emqx_types:subopts()}}.
client_subscribe(GwName, ClientId, Topic, SubOpts) ->
    client_call(GwName, ClientId, {subscribe, Topic, SubOpts}).

-spec client_unsubscribe(
    gateway_name(),
    emqx_types:clientid(),
    emqx_types:topic()
) ->
    {error, any()}
    | ok.
client_unsubscribe(GwName, ClientId, Topic) ->
    client_call(GwName, ClientId, {unsubscribe, Topic}).

client_call(GwName, ClientId, Req) ->
    try
        emqx_gateway_cm:call(
            GwName,
            ClientId,
            Req,
            ?DEFAULT_CALL_TIMEOUT
        )
    of
        undefined ->
            {error, not_found};
        Res ->
            Res
    catch
        throw:noproc ->
            {error, not_found};
        throw:{badrpc, Reason} ->
            {error, {badrpc, Reason}}
    end.

%%--------------------------------------------------------------------
%% Utils
%%--------------------------------------------------------------------

-spec reason2resp({atom(), map()} | any()) -> binary() | any().
reason2resp(R) ->
    case reason2msg(R) of
        error ->
            return_http_error(500, R);
        Msg ->
            return_http_error(400, Msg)
    end.

-spec return_http_error(integer(), any()) -> {integer(), atom(), binary()}.
return_http_error(Code, Msg) ->
    {Code, codestr(Code), emqx_gateway_utils:stringfy(Msg)}.

-spec reason2msg({atom(), map()} | any()) -> error | string().
reason2msg({badconf, #{key := Key, value := Value, reason := Reason}}) ->
    NValue =
        case emqx_json:safe_encode(Value) of
            {ok, Str} -> Str;
            {error, _} -> emqx_gateway_utils:stringfy(Value)
        end,
    fmtstr(
        "Bad config value '~s' for '~s', reason: ~s",
        [NValue, Key, emqx_gateway_utils:stringfy(Reason)]
    );
reason2msg(
    {badres, #{
        resource := gateway,
        gateway := GwName,
        reason := not_found
    }}
) ->
    fmtstr("The ~s gateway is unloaded", [GwName]);
reason2msg(
    {badres, #{
        resource := gateway,
        gateway := GwName,
        reason := already_exist
    }}
) ->
    fmtstr("The ~s gateway already loaded", [GwName]);
reason2msg(
    {badres, #{
        resource := listener,
        listener := {GwName, LType, LName},
        reason := not_found
    }}
) ->
    fmtstr("Listener ~s not found", [listener_id(GwName, LType, LName)]);
reason2msg(
    {badres, #{
        resource := listener,
        listener := {GwName, LType, LName},
        reason := already_exist
    }}
) ->
    fmtstr(
        "The listener ~s of ~s already exist",
        [listener_id(GwName, LType, LName), GwName]
    );
reason2msg(
    {badres, #{
        resource := authn,
        gateway := GwName,
        reason := not_found
    }}
) ->
    fmtstr("The authentication not found on ~s", [GwName]);
reason2msg(
    {badres, #{
        resource := authn,
        gateway := GwName,
        reason := already_exist
    }}
) ->
    fmtstr("The authentication already exist on ~s", [GwName]);
reason2msg(
    {badres, #{
        resource := listener_authn,
        listener := {GwName, LType, LName},
        reason := not_found
    }}
) ->
    fmtstr(
        "The authentication not found on ~s",
        [listener_id(GwName, LType, LName)]
    );
reason2msg(
    {badres, #{
        resource := listener_authn,
        listener := {GwName, LType, LName},
        reason := already_exist
    }}
) ->
    fmtstr(
        "The authentication already exist on ~s",
        [listener_id(GwName, LType, LName)]
    );
reason2msg(
    {bad_ssl_config, #{
        reason := Reason,
        which_options := Options
    }}
) ->
    fmtstr("Bad TLS configuration for ~p, reason: ~s", [Options, Reason]);
reason2msg(
    {#{roots := [{gateway, _}]}, [_ | _]} = Error
) ->
    Bin = emqx_misc:readable_error_msg(Error),
    <<"Invalid configurations: ", Bin/binary>>;
reason2msg(_) ->
    error.

codestr(400) -> 'BAD_REQUEST';
codestr(404) -> 'RESOURCE_NOT_FOUND';
codestr(405) -> 'METHOD_NOT_ALLOWED';
codestr(409) -> 'NOT_SUPPORT';
codestr(500) -> 'UNKNOW_ERROR';
codestr(501) -> 'NOT_IMPLEMENTED'.

fmtstr(Fmt, Args) ->
    lists:flatten(io_lib:format(Fmt, Args)).

-spec with_authn(binary(), function()) -> any().
with_authn(GwName0, Fun) ->
    with_gateway(GwName0, fun(GwName, _GwConf) ->
        Authn = emqx_gateway_http:authn(GwName),
        Fun(GwName, Authn)
    end).

-spec with_listener_authn(binary(), binary(), function()) -> any().
with_listener_authn(GwName0, Id, Fun) ->
    with_gateway(GwName0, fun(GwName, _GwConf) ->
        Authn = emqx_gateway_http:authn(GwName, Id),
        Fun(GwName, Authn)
    end).

-spec with_gateway(binary(), function()) -> any().
with_gateway(GwName0, Fun) ->
    try
        GwName =
            try
                binary_to_existing_atom(GwName0)
            catch
                _:_ -> error(badname)
            end,
        case emqx_gateway:lookup(GwName) of
            undefined ->
                return_http_error(404, "Gateway not loaded");
            Gateway ->
                Fun(GwName, Gateway)
        end
    catch
        error:badname ->
            return_http_error(404, "Bad gateway name");
        %% Exceptions from: checks/2
        error:{miss_param, K} ->
            return_http_error(400, [K, " is required"]);
        %% Exceptions from emqx_gateway_utils:parse_listener_id/1
        error:{invalid_listener_id, Id} ->
            return_http_error(400, ["Invalid listener id: ", Id]);
        %% Exceptions from emqx:get_config/1
        error:{config_not_found, Path0} ->
            Path = lists:concat(
                lists:join(".", lists:map(fun to_list/1, Path0))
            ),
            return_http_error(404, "Resource not found. path: " ++ Path);
        error:{badmatch, {error, einval}} ->
            return_http_error(400, "Invalid bind address");
        error:{badauth, Reason} ->
            Reason1 = emqx_gateway_utils:stringfy(Reason),
            return_http_error(400, ["Bad authentication config: ", Reason1]);
        Class:Reason:Stk ->
            ?SLOG(error, #{
                msg => "uncaught_exception",
                exception => Class,
                reason => Reason,
                stacktrace => Stk
            }),
            reason2resp(Reason)
    end.

-spec checks(list(), map()) -> ok.
checks([], _) ->
    ok;
checks([K | Ks], Map) ->
    case maps:is_key(K, Map) of
        true -> checks(Ks, Map);
        false -> error({miss_param, K})
    end.

to_list(A) when is_atom(A) ->
    atom_to_list(A);
to_list(B) when is_binary(B) ->
    binary_to_list(B).

sum_cluster_connections(List) ->
    sum_cluster_connections(List, 0, 0).

%%--------------------------------------------------------------------
%% Internal funcs
sum_cluster_connections(
    [#{max_connections := Max, current_connections := Current} | T], MaxAcc, CurrAcc
) ->
    sum_cluster_connections(T, MaxAcc + Max, Current + CurrAcc);
sum_cluster_connections([_ | T], MaxAcc, CurrAcc) ->
    sum_cluster_connections(T, MaxAcc, CurrAcc);
sum_cluster_connections([], MaxAcc, CurrAcc) ->
    {MaxAcc, CurrAcc}.
