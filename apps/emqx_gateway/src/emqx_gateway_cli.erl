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

%% @doc The Command-Line-Interface module for Gateway Application
-module(emqx_gateway_cli).

-export([load/0]).

-export([ gateway/1
        , 'gateway-registry'/1
        , 'gateway-clients'/1
        , 'gateway-metrics'/1
        %, 'gateway-banned'/1
        ]).

-spec(load() -> ok).
load() ->
    Cmds = [Fun || {Fun, _} <- ?MODULE:module_info(exports), is_cmd(Fun)],
    lists:foreach(fun(Cmd) -> emqx_ctl:register_command(Cmd, {?MODULE, Cmd}, []) end, Cmds).

is_cmd(Fun) ->
    not lists:member(Fun, [init, load, module_info]).

%%--------------------------------------------------------------------
%% Cmds

gateway(["list"]) ->
    lists:foreach(fun(#{id := InstaId, name := Name, gwid := GwId}) ->
        %% FIXME: Get the real running status
        emqx_ctl:print("Gateway(~s, name=~s, gwid=~s, status=running",
                       [InstaId, Name, GwId])
    end, emqx_gateway:list());

gateway(["lookup", GatewayInstaId]) ->
    case emqx_gateway:lookup(GatewayInstaId) of
        undefined ->
            emqx_ctl:print("undefined");
        Info ->
            emqx_ctl:print("~p~n", [Info])
    end;

gateway(["stop", GatewayInstaId]) ->
    case emqx_gateway:stop(GatewayInstaId) of
        ok ->
            emqx_ctl:print("ok");
        {error, Reason} ->
            emqx_ctl:print("Error: ~p~n", [Reason])
    end;

gateway(["start", GatewayInstaId]) ->
    case emqx_gateway:start(GatewayInstaId) of
        ok ->
            emqx_ctl:print("ok");
        {error, Reason} ->
            emqx_ctl:print("Error: ~p~n", [Reason])
    end;

gateway(_) ->
    %% TODO: create/remove APIs
    emqx_ctl:usage([ {"gateway list",
                        "List all created gateway instances"}
                   , {"gateway lookup <GatewayId>",
                        "Looup a gateway detailed informations"}
                   , {"gateway stop   <GatewayId>",
                        "Stop a gateway instance and release all resources"}
                   , {"gateway start  <GatewayId>",
                        "Start a gateway instance"}
                   ]).

'gateway-registry'(["list"]) ->
    lists:foreach(
      fun({GwType, #{cbkmod := CbMod}}) ->
        emqx_ctl:print("Registered Type: ~s, Callback Module: ~s", [GwType, CbMod])
      end,
    emqx_gateway_registry:list());

'gateway-registry'(_) ->
    emqx_ctl:usage([ {"gateway-registry list",
                        "List all registered gateway types"}
                   ]).

'gateway-clients'(["list", Type]) ->
    InfoTab = emqx_gateway_cm:tabname(info, Type),
    dump(InfoTab, client);

'gateway-clients'(["lookup", Type, ClientId]) ->
    InfoTab = emqx_gateway_cm:tabname(info, Type),
    case ets:lookup(InfoTab, bin(ClientId)) of
        [] -> emqx_ctl:print("Not Found.~n");
        [ChannInfo] ->
            print({client, ChannInfo})
    end;

'gateway-clients'(["kick", Type, ClientId]) ->
    case emqx_cm:kick_session(Type, bin(ClientId)) of
        ok -> emqx_ctl:print("ok~n");
        _ -> emqx_ctl:print("Not Found.~n")
    end;

'gateway-clients'(_) ->
    emqx_ctl:usage([ {"gateway-clients list   <Type>",
                        "List all clients for a type of gateway"}
                   , {"gateway-clients lookup <Type> <ClientId>",
                        "Lookup the Client Info for specified client"}
                   , {"gateway-clients kick   <Type> <ClientId>",
                        "Kick out a client"}
                   ]).

'gateway-metrics'([GatewayType]) ->
    Tab = emqx_gateway_metrics:tabname(GatewayType),
    lists:foreach(
      fun({K, V}) ->
        emqx_ctl:print("~-48s: ~w", [K, V])
      end, ets:tab2list(Tab));

'gateway-metrics'(_) ->
    emqx_ctl:usage([ {"gateway-metrics <Type>",
                        "List all metrics for a type of gateway"}
                   ]).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

bin(S) -> iolist_to_binary(S).

dump(Table, Tag) ->
    dump(Table, Tag, ets:first(Table), []).

dump(_Table, _, '$end_of_table', Result) ->
    lists:reverse(Result);

dump(Table, Tag, Key, Result) ->
    PrintValue = [print({Tag, Record}) || Record <- ets:lookup(Table, Key)],
    dump(Table, Tag, ets:next(Table, Key), [PrintValue | Result]).

print({client, {_, Infos, Stats}}) ->
    ClientInfo = maps:get(clientinfo, Infos, #{}),
    ConnInfo   = maps:get(conninfo, Infos, #{}),
    _Session    = maps:get(session, Infos, #{}),
    SafeGet    = fun(K, M) -> maps:get(K, M, undefined) end,
    StatsGet   = fun(K) -> proplists:get_value(K, Stats, 0) end,

    ConnectedAt = SafeGet(connected_at, ConnInfo),
    InfoKeys = [clientid, username, peername, clean_start, keepalive,
                subscriptions_cnt, send_msg, connected, created_at, connected_at],
    Info = #{ clientid => SafeGet(clientid, ClientInfo),
              username => SafeGet(username, ClientInfo),
              peername => SafeGet(peername, ConnInfo),
              clean_start => SafeGet(clean_start, ConnInfo),
              keepalive => SafeGet(keepalive, ConnInfo),
              subscriptions_cnt => StatsGet(subscriptions_cnt),
              send_msg => StatsGet(send_msg),
              connected => SafeGet(conn_state, ClientInfo) == connected,
              created_at => ConnectedAt,
              connected_at => ConnectedAt
            },
    emqx_ctl:print("Client(~s, username=~s, peername=~s, "
                   "clean_start=~s, keepalive=~w,"
                   "subscriptions=~w, delivered_msgs=~w,"
                   "connected=~s, created_at=~w, connected_at=~w" ++ case maps:is_key(disconnected_at, Info) of
                                                                      true  -> ", disconnected_at=~w)~n";
                                                                      false -> ")~n"
                                                                  end,
                [format(K, maps:get(K, Info)) || K <- InfoKeys]).

format(_, undefined) ->
    undefined;

format(peername, {IPAddr, Port}) ->
    IPStr = emqx_mgmt_util:ntoa(IPAddr),
    io_lib:format("~s:~p", [IPStr, Port]);

format(_, Val) ->
    Val.
