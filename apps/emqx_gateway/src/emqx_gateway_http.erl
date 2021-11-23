%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Mgmt APIs - gateway
-export([ gateways/1
        ]).

%% Mgmt APIs
-export([ add_listener/2
        , remove_listener/1
        , update_listener/2
        ]).

-export([ authn/1
        , authn/2
        , add_authn/2
        , add_authn/3
        , update_authn/2
        , update_authn/3
        , remove_authn/1
        , remove_authn/2
        ]).

%% Mgmt APIs - clients
-export([ lookup_client/3
        , lookup_client/4
        , kickout_client/2
        , kickout_client/3
        , list_client_subscriptions/2
        , client_subscribe/4
        , client_unsubscribe/3
        ]).

%% Utils for http, swagger, etc.
-export([ return_http_error/2
        , with_gateway/2
        , with_authn/2
        , with_listener_authn/3
        , checks/2
        , schema_bad_request/0
        , schema_not_found/0
        , schema_internal_error/0
        , schema_no_content/0
        ]).

-type gateway_summary() ::
        #{ name := binary()
         , status := running | stopped | unloaded
         , started_at => binary()
         , max_connections => integer()
         , current_connections => integer()
         , listeners => []
         }.

-elvis([{elvis_style, god_modules, disable}]).
-elvis([{elvis_style, no_nested_try_catch, disable}]).


-define(DEFAULT_CALL_TIMEOUT, 15000).

%%--------------------------------------------------------------------
%% Mgmt APIs - gateway
%%--------------------------------------------------------------------

-spec gateways(Status :: all | running | stopped | unloaded)
    -> [gateway_summary()].
gateways(Status) ->
    Gateways = lists:map(fun({GwName, _}) ->
        case emqx_gateway:lookup(GwName) of
            undefined -> #{name => GwName, status => unloaded};
            GwInfo = #{config := Config} ->
                GwInfo0 = emqx_gateway_utils:unix_ts_to_rfc3339(
                            [created_at, started_at, stopped_at],
                            GwInfo),
                GwInfo1 = maps:with([name,
                                     status,
                                     created_at,
                                     started_at,
                                     stopped_at], GwInfo0),
                GwInfo1#{
                  max_connections => max_connections_count(Config),
                  current_connections => current_connections_count(GwName),
                  listeners => get_listeners_status(GwName, Config)}
        end
    end, emqx_gateway_registry:list()),
    case Status of
        all -> Gateways;
        _ ->
            [Gw || Gw = #{status := S} <- Gateways, S == Status]
    end.

%% @private
max_connections_count(Config) ->
    Listeners = emqx_gateway_utils:normalize_config(Config),
    lists:foldl(fun({_, _, _, SocketOpts, _}, Acc) ->
        Acc + proplists:get_value(max_connections, SocketOpts, 0)
    end, 0, Listeners).

%% @private
current_connections_count(GwName) ->
    try
        InfoTab = emqx_gateway_cm:tabname(info, GwName),
        ets:info(InfoTab, size)
    catch _ : _ ->
        0
    end.

%% @private
get_listeners_status(GwName, Config) ->
    Listeners = emqx_gateway_utils:normalize_config(Config),
    lists:map(fun({Type, LisName, ListenOn, _, _}) ->
        Name0 = emqx_gateway_utils:listener_id(GwName, Type, LisName),
        Name = {Name0, ListenOn},
        LisO = #{id => Name0, type => Type, name => LisName},
        case catch esockd:listener(Name) of
            _Pid when is_pid(_Pid) ->
                LisO#{running => true};
            _ ->
                LisO#{running => false}
        end
    end, Listeners).

%%--------------------------------------------------------------------
%% Mgmt APIs - listeners
%%--------------------------------------------------------------------

-spec add_listener(atom() | binary(), map()) -> ok.
add_listener(ListenerId, NewConf0) ->
    {GwName, Type, Name} = emqx_gateway_utils:parse_listener_id(ListenerId),
    NewConf = maps:without([<<"id">>, <<"name">>,
                            <<"type">>, <<"running">>], NewConf0),
    confexp(emqx_gateway_conf:add_listener(GwName, {Type, Name}, NewConf)).

-spec update_listener(atom() | binary(), map()) -> ok.
update_listener(ListenerId, NewConf0) ->
    {GwName, Type, Name} = emqx_gateway_utils:parse_listener_id(ListenerId),

    NewConf = maps:without([<<"id">>, <<"name">>,
                            <<"type">>, <<"running">>], NewConf0),
    confexp(emqx_gateway_conf:update_listener(GwName, {Type, Name}, NewConf)).

-spec remove_listener(binary()) -> ok.
remove_listener(ListenerId) ->
    {GwName, Type, Name} = emqx_gateway_utils:parse_listener_id(ListenerId),
    confexp(emqx_gateway_conf:remove_listener(GwName, {Type, Name})).

-spec authn(gateway_name()) -> map().
authn(GwName) ->
    %% XXX: Need append chain-nanme, authenticator-id?
    Path = [gateway, GwName, authentication],
    ChainName = emqx_gateway_utils:global_chain(GwName),
    wrap_chain_name(
      ChainName,
      emqx_map_lib:jsonable_map(emqx:get_config(Path))
     ).

-spec authn(gateway_name(), binary()) -> map().
authn(GwName, ListenerId) ->
    {_, Type, Name} = emqx_gateway_utils:parse_listener_id(ListenerId),
    Path = [gateway, GwName, listeners, Type, Name, authentication],
    ChainName = emqx_gateway_utils:listener_chain(GwName, Type, Name),
    wrap_chain_name(
      ChainName,
      emqx_map_lib:jsonable_map(emqx:get_config(Path))
     ).

wrap_chain_name(ChainName, Conf) ->
    case emqx_authentication:list_authenticators(ChainName) of
        {ok, [#{id := Id} | _]} ->
            Conf#{chain_name => ChainName, id => Id};
        _ ->
            Conf
    end.

-spec add_authn(gateway_name(), map()) -> ok.
add_authn(GwName, AuthConf) ->
    confexp(emqx_gateway_conf:add_authn(GwName, AuthConf)).

-spec add_authn(gateway_name(), binary(), map()) -> ok.
add_authn(GwName, ListenerId, AuthConf) ->
    {_, Type, Name} = emqx_gateway_utils:parse_listener_id(ListenerId),
    confexp(emqx_gateway_conf:add_authn(GwName, {Type, Name}, AuthConf)).

-spec update_authn(gateway_name(), map()) -> ok.
update_authn(GwName, AuthConf) ->
    confexp(emqx_gateway_conf:update_authn(GwName, AuthConf)).

-spec update_authn(gateway_name(), binary(), map()) -> ok.
update_authn(GwName, ListenerId, AuthConf) ->
    {_, Type, Name} = emqx_gateway_utils:parse_listener_id(ListenerId),
    confexp(emqx_gateway_conf:update_authn(GwName, {Type, Name}, AuthConf)).

-spec remove_authn(gateway_name()) -> ok.
remove_authn(GwName) ->
    confexp(emqx_gateway_conf:remove_authn(GwName)).

-spec remove_authn(gateway_name(), binary()) -> ok.
remove_authn(GwName, ListenerId) ->
    {_, Type, Name} = emqx_gateway_utils:parse_listener_id(ListenerId),
    confexp(emqx_gateway_conf:remove_authn(GwName, {Type, Name})).

confexp(ok) -> ok;
confexp({error, not_found}) ->
    error({update_conf_error, not_found});
confexp({error, already_exist}) ->
    error({update_conf_error, already_exist}).

%%--------------------------------------------------------------------
%% Mgmt APIs - clients
%%--------------------------------------------------------------------

-spec lookup_client(gateway_name(),
                    emqx_type:clientid(), {atom(), atom()}) -> list().
lookup_client(GwName, ClientId, FormatFun) ->
    lists:append([lookup_client(Node, GwName, {clientid, ClientId}, FormatFun)
                  || Node <- mria_mnesia:running_nodes()]).

lookup_client(Node, GwName, {clientid, ClientId}, {M,F}) when Node =:= node() ->
    ChanTab = emqx_gateway_cm:tabname(chan, GwName),
    InfoTab = emqx_gateway_cm:tabname(info, GwName),

    lists:append(lists:map(
      fun(Key) ->
        lists:map(fun M:F/1, ets:lookup(InfoTab, Key))
      end, ets:lookup(ChanTab, ClientId)));

lookup_client(Node, GwName, {clientid, ClientId}, FormatFun) ->
    rpc_call(Node, lookup_client,
             [Node, GwName, {clientid, ClientId}, FormatFun]).

-spec kickout_client(gateway_name(), emqx_type:clientid())
    -> {error, any()}
     | ok.
kickout_client(GwName, ClientId) ->
    Results = [kickout_client(Node, GwName, ClientId)
               || Node <- mria_mnesia:running_nodes()],
    case lists:any(fun(Item) -> Item =:= ok end, Results) of
        true  -> ok;
        false -> lists:last(Results)
    end.

kickout_client(Node, GwName, ClientId) when Node =:= node() ->
    emqx_gateway_cm:kick_session(GwName, ClientId);

kickout_client(Node, GwName, ClientId) ->
    rpc_call(Node, kickout_client, [Node, GwName, ClientId]).

-spec list_client_subscriptions(gateway_name(), emqx_type:clientid())
    -> {error, any()}
     | {ok, list()}.
list_client_subscriptions(GwName, ClientId) ->
    %% Get the subscriptions from session-info
    with_channel(GwName, ClientId,
        fun(Pid) ->
            Subs = emqx_gateway_conn:call(
                     Pid,
                     subscriptions, ?DEFAULT_CALL_TIMEOUT),
            {ok, lists:map(fun({Topic, SubOpts}) ->
                     SubOpts#{topic => Topic}
                 end, Subs)}
        end).

-spec client_subscribe(gateway_name(), emqx_type:clientid(),
                       emqx_type:topic(), emqx_type:subopts())
    -> {error, any()}
     | ok.
client_subscribe(GwName, ClientId, Topic, SubOpts) ->
    with_channel(GwName, ClientId,
        fun(Pid) ->
            emqx_gateway_conn:call(
              Pid, {subscribe, Topic, SubOpts},
              ?DEFAULT_CALL_TIMEOUT
             )
        end).

-spec client_unsubscribe(gateway_name(),
                         emqx_type:clientid(), emqx_type:topic())
    -> {error, any()}
     | ok.
client_unsubscribe(GwName, ClientId, Topic) ->
    with_channel(GwName, ClientId,
        fun(Pid) ->
            emqx_gateway_conn:call(
              Pid, {unsubscribe, Topic}, ?DEFAULT_CALL_TIMEOUT)
        end).

with_channel(GwName, ClientId, Fun) ->
    case emqx_gateway_cm:with_channel(GwName, ClientId, Fun) of
        undefined -> {error, not_found};
        Res -> Res
    end.

%%--------------------------------------------------------------------
%% Utils
%%--------------------------------------------------------------------

-spec return_http_error(integer(), any()) -> {integer(), binary()}.
return_http_error(Code, Msg) ->
    {Code, emqx_json:encode(
             #{code => codestr(Code),
               message => emqx_gateway_utils:stringfy(Msg)
              })
    }.

codestr(400) -> 'BAD_REQUEST';
codestr(401) -> 'NOT_SUPPORTED_NOW';
codestr(404) -> 'RESOURCE_NOT_FOUND';
codestr(500) -> 'UNKNOW_ERROR'.

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
        GwName = try
                     binary_to_existing_atom(GwName0)
                 catch _ : _ -> error(badname)
                 end,
        case emqx_gateway:lookup(GwName) of
            undefined ->
                return_http_error(404, "Gateway not load");
            Gateway ->
                Fun(GwName, Gateway)
        end
    catch
        error : badname ->
            return_http_error(404, "Bad gateway name");
        %% Exceptions from: checks/2
        error : {miss_param, K} ->
            return_http_error(400, [K, " is required"]);
        %% Exceptions from emqx_gateway_utils:parse_listener_id/1
        error : {invalid_listener_id, Id} ->
            return_http_error(400, ["invalid listener id: ", Id]);
        %% Exceptions from: emqx:get_config/1
        error : {config_not_found, Path0} ->
            Path = lists:concat(
                     lists:join(".", lists:map(fun to_list/1, Path0))),
            return_http_error(404, "Resource not found. path: " ++ Path);
        %% Exceptions from: confexp/1
        error : {update_conf_error, not_found} ->
            return_http_error(404, "Resource not found");
        error : {update_conf_error, already_exist} ->
            return_http_error(400, "Resource already exist");
        Class : Reason : Stk ->
            ?SLOG(error, #{ msg => "uncatched_error"
                          , reason => {Class, Reason}
                          , stacktrace => Stk
                          }),
            return_http_error(500, {Class, Reason, Stk})
    end.

-spec checks(list(), map()) -> ok.
checks([], _) ->
    ok;
checks([K | Ks], Map) ->
    case maps:is_key(K, Map) of
        true -> checks(Ks, Map);
        false ->
            error({miss_param, K})
    end.

to_list(A) when is_atom(A) ->
    atom_to_list(A);
to_list(B) when is_binary(B) ->
    binary_to_list(B).

%%--------------------------------------------------------------------
%% common schemas

schema_bad_request() ->
    emqx_mgmt_util:error_schema(
      <<"Some Params missed">>, ['PARAMETER_MISSED']).
schema_internal_error() ->
    emqx_mgmt_util:error_schema(
      <<"Ineternal Server Error">>, ['INTERNAL_SERVER_ERROR']).
schema_not_found() ->
    emqx_mgmt_util:error_schema(<<"Resource Not Found">>).
schema_no_content() ->
    #{description => <<"No Content">>}.

%%--------------------------------------------------------------------
%% Internal funcs

rpc_call(Node, Fun, Args) ->
    case rpc:call(Node, ?MODULE, Fun, Args) of
        {badrpc, Reason} -> {error, Reason};
        Res -> Res
    end.
