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

%% Mgmt APIs - listeners
-export([ listeners/1
        , listener/2
        , mapping_listener_m2l/2
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
        ]).

-type gateway_summary() ::
        #{ name := binary()
         , status := running | stopped | unloaded
         , started_at => binary()
         , max_connections => integer()
         , current_connections => integer()
         , listeners => []
         }.

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
        LisO = #{id => Name0, type => Type},
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

listeners(GwName) when is_atom (GwName) ->
    listeners(atom_to_binary(GwName));
listeners(GwName) ->
    RawConf = emqx_config:fill_defaults(
                emqx_config:get_root_raw([<<"gateway">>])),
    Listeners = emqx_map_lib:jsonable_map(
                  emqx_map_lib:deep_get(
                    [<<"gateway">>, GwName, <<"listeners">>], RawConf)),
    mapping_listener_m2l(GwName, Listeners).

listener(_GwName, _ListenerId) ->
    ok.

mapping_listener_m2l(GwName, Listeners0) ->
    Listeners = maps:to_list(Listeners0),
    lists:append([listener(GwName, Type, maps:to_list(Conf))
                  || {Type, Conf} <- Listeners]).

listener(GwName, Type, Conf) ->
    [begin
         ListenerId = emqx_gateway_utils:listener_id(GwName, Type, LName),
         Running = is_running(ListenerId, LConf),
         LConf#{
           id => ListenerId,
           type => Type,
           running => Running
          }
     end || {LName, LConf} <- Conf, is_map(LConf)].

is_running(ListenerId, #{<<"bind">> := ListenOn0}) ->
    ListenOn = emqx_gateway_utils:parse_listenon(ListenOn0),
    try esockd:listener({ListenerId, ListenOn}) of
        Pid when is_pid(Pid)->
            true
    catch _:_ ->
        false
    end.

%%--------------------------------------------------------------------
%% Mgmt APIs - clients
%%--------------------------------------------------------------------

-spec lookup_client(gateway_name(),
                    emqx_type:clientid(), {atom(), atom()}) -> list().
lookup_client(GwName, ClientId, FormatFun) ->
    lists:append([lookup_client(Node, GwName, {clientid, ClientId}, FormatFun)
                  || Node <- ekka_mnesia:running_nodes()]).

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
               || Node <- ekka_mnesia:running_nodes()],
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

-spec return_http_error(integer(), binary()) -> {integer(), binary()}.
return_http_error(Code, Msg) ->
    {Code, emqx_json:encode(
             #{code => codestr(Code),
               reason => emqx_gateway_utils:stringfy(Msg)
              })
    }.

codestr(404) -> 'RESOURCE_NOT_FOUND';
codestr(401) -> 'NOT_SUPPORTED_NOW';
codestr(500) -> 'UNKNOW_ERROR'.

%%--------------------------------------------------------------------
%% Internal funcs

rpc_call(Node, Fun, Args) ->
    case rpc:call(Node, ?MODULE, Fun, Args) of
        {badrpc, Reason} -> {error, Reason};
        Res -> Res
    end.
