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

-export([ load/0
        , unload/0
        ]).

-export([ gateway/1
        , 'gateway-registry'/1
        , 'gateway-clients'/1
        , 'gateway-metrics'/1
        %, 'gateway-banned'/1
        ]).

-spec load() -> ok.
load() ->
    Cmds = [Fun || {Fun, _} <- ?MODULE:module_info(exports), is_cmd(Fun)],
    lists:foreach(fun(Cmd) ->
        emqx_ctl:register_command(Cmd, {?MODULE, Cmd}, [])
     end, Cmds).

-spec unload() -> ok.
unload() ->
    Cmds = [Fun || {Fun, _} <- ?MODULE:module_info(exports), is_cmd(Fun)],
    lists:foreach(fun(Cmd) -> emqx_ctl:unregister_command(Cmd) end, Cmds).

is_cmd(Fun) ->
    case atom_to_list(Fun) of
        "gateway" ++ _ -> true;
        _ -> false
    end.

%%--------------------------------------------------------------------
%% Cmds

gateway(["list"]) ->
    lists:foreach(fun(#{name := Name} = Gateway) ->
        %% TODO: More infos: listeners?, connected?
        Status = maps:get(status, Gateway, stopped),
        emqx_ctl:print("Gateway(name=~s, status=~s)~n",
                       [Name, Status])
    end, emqx_gateway:list());

gateway(["lookup", Name]) ->
    case emqx_gateway:lookup(atom(Name)) of
        undefined ->
            emqx_ctl:print("undefined~n");
        Info ->
            emqx_ctl:print("~p~n", [Info])
    end;

gateway(["stop", Name]) ->
    case emqx_gateway:stop(atom(Name)) of
        ok ->
            emqx_ctl:print("ok~n");
        {error, Reason} ->
            emqx_ctl:print("Error: ~p~n", [Reason])
    end;

gateway(["start", Name]) ->
    case emqx_gateway:start(atom(Name)) of
        ok ->
            emqx_ctl:print("ok~n");
        {error, Reason} ->
            emqx_ctl:print("Error: ~p~n", [Reason])
    end;

gateway(_) ->
    %% TODO: create/remove APIs
    emqx_ctl:usage([ {"gateway list",
                        "List all gateway"}
                   , {"gateway lookup <Name>",
                        "Lookup a gateway detailed informations"}
                   , {"gateway stop   <Name>",
                        "Stop a gateway instance"}
                   , {"gateway start  <Name>",
                        "Start a gateway instance"}
                   ]).

'gateway-registry'(["list"]) ->
    lists:foreach(
      fun({Name, #{cbkmod := CbMod}}) ->
        emqx_ctl:print("Registered Name: ~s, Callback Module: ~s~n", [Name, CbMod])
      end,
    emqx_gateway_registry:list());

'gateway-registry'(_) ->
    emqx_ctl:usage([ {"gateway-registry list",
                        "List all registered gateways"}
                   ]).

'gateway-clients'(["list", Name]) ->
    %% FIXME: page me. for example: --limit 100 --page 10 ???
    InfoTab = emqx_gateway_cm:tabname(info, Name),
    case ets:info(InfoTab) of
        undefined ->
            emqx_ctl:print("Bad Gateway Name.~n");
        _ ->
        dump(InfoTab, client)
    end;

'gateway-clients'(["lookup", Name, ClientId]) ->
    ChanTab = emqx_gateway_cm:tabname(chan, Name),
    case ets:lookup(ChanTab, bin(ClientId)) of
        [] -> emqx_ctl:print("Not Found.~n");
        [Chann] ->
            InfoTab = emqx_gateway_cm:tabname(info, Name),
            [ChannInfo] = ets:lookup(InfoTab, Chann),
            print({client, ChannInfo})
    end;

'gateway-clients'(["kick", Name, ClientId]) ->
    case emqx_gateway_cm:kick_session(Name, bin(ClientId)) of
        ok -> emqx_ctl:print("ok~n");
        _ -> emqx_ctl:print("Not Found.~n")
    end;

'gateway-clients'(_) ->
    emqx_ctl:usage([ {"gateway-clients list   <Name>",
                        "List all clients for a gateway"}
                   , {"gateway-clients lookup <Name> <ClientId>",
                        "Lookup the Client Info for specified client"}
                   , {"gateway-clients kick   <Name> <ClientId>",
                        "Kick out a client"}
                   ]).

'gateway-metrics'([Name]) ->
    Tab = emqx_gateway_metrics:tabname(Name),
    case ets:info(Tab) of
        undefined ->
            emqx_ctl:print("Bad Gateway Name.~n");
        _ ->
            lists:foreach(
              fun({K, V}) ->
                emqx_ctl:print("~-30s: ~w~n", [K, V])
              end, lists:sort(ets:tab2list(Tab)))
    end;

'gateway-metrics'(_) ->
    emqx_ctl:usage([ {"gateway-metrics <Name>",
                        "List all metrics for a gateway"}
                   ]).

atom(Id) ->
    try
        list_to_existing_atom(Id)
    catch
        _ : _ -> undefined
    end.

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
              connected => SafeGet(conn_state, Infos) == connected,
              created_at => ConnectedAt,
              connected_at => ConnectedAt
            },

    emqx_ctl:print("Client(~s, username=~s, peername=~s, "
                   "clean_start=~s, keepalive=~w, "
                   "subscriptions=~w, delivered_msgs=~w, "
                   "connected=~s, created_at=~w, connected_at=~w)~n",
                [format(K, maps:get(K, Info)) || K <- InfoKeys]).

format(_, undefined) ->
    undefined;

format(peername, {IPAddr, Port}) ->
    IPStr = emqx_mgmt_util:ntoa(IPAddr),
    io_lib:format("~s:~p", [IPStr, Port]);

format(_, Val) ->
    Val.
