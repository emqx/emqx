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

%% @doc The Command-Line-Interface module for Gateway Application
-module(emqx_gateway_cli).

-export([
    load/0,
    unload/0
]).

-export([
    gateway/1,
    'gateway-registry'/1,
    'gateway-clients'/1,
    'gateway-metrics'/1
    %, 'gateway-banned'/1
]).

-elvis([{elvis_style, function_naming_convention, disable}]).

-spec load() -> ok.
load() ->
    Cmds = [Fun || {Fun, _} <- ?MODULE:module_info(exports), is_cmd(Fun)],
    lists:foreach(
        fun(Cmd) ->
            emqx_ctl:register_command(Cmd, {?MODULE, Cmd}, [])
        end,
        Cmds
    ).

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
    lists:foreach(
        fun(GwSummary) ->
            print(format_gw_summary(GwSummary))
        end,
        emqx_gateway_http:gateways(all)
    );
gateway(["lookup", Name]) ->
    case emqx_gateway:lookup(atom(Name)) of
        undefined ->
            print("undefined\n");
        Gateway ->
            print(format_gateway(Gateway))
    end;
gateway(["load", Name, Conf]) ->
    case
        emqx_gateway_conf:load_gateway(
            bin(Name),
            emqx_json:decode(Conf, [return_maps])
        )
    of
        {ok, _} ->
            print("ok\n");
        {error, Reason} ->
            print("Error: ~ts\n", [format_error(Reason)])
    end;
gateway(["unload", Name]) ->
    case emqx_gateway_conf:unload_gateway(bin(Name)) of
        ok ->
            print("ok\n");
        {error, Reason} ->
            print("Error: ~ts\n", [format_error(Reason)])
    end;
gateway(["stop", Name]) ->
    case
        emqx_gateway_conf:update_gateway(
            bin(Name),
            #{<<"enable">> => <<"false">>}
        )
    of
        {ok, _} ->
            print("ok\n");
        {error, Reason} ->
            print("Error: ~ts\n", [format_error(Reason)])
    end;
gateway(["start", Name]) ->
    case
        emqx_gateway_conf:update_gateway(
            bin(Name),
            #{<<"enable">> => <<"true">>}
        )
    of
        {ok, _} ->
            print("ok\n");
        {error, Reason} ->
            print("Error: ~ts\n", [format_error(Reason)])
    end;
gateway(_) ->
    emqx_ctl:usage(
        [
            {"gateway list", "List all gateway"},
            {"gateway lookup <Name>", "Lookup a gateway detailed information"},
            {"gateway load   <Name> <JsonConf>", "Load a gateway with config"},
            {"gateway unload <Name>", "Unload the gateway"},
            {"gateway stop   <Name>", "Stop the gateway"},
            {"gateway start  <Name>", "Start the gateway"}
        ]
    ).

'gateway-registry'(["list"]) ->
    lists:foreach(
        fun({Name, #{cbkmod := CbMod}}) ->
            print("Registered Name: ~ts, Callback Module: ~ts\n", [Name, CbMod])
        end,
        emqx_gateway_registry:list()
    );
'gateway-registry'(_) ->
    emqx_ctl:usage([{"gateway-registry list", "List all registered gateways"}]).

'gateway-clients'(["list", Name]) ->
    %% XXX: page me?
    InfoTab = emqx_gateway_cm:tabname(info, Name),
    case ets:info(InfoTab) of
        undefined ->
            print("Bad Gateway Name.\n");
        _ ->
            dump(InfoTab, client)
    end;
'gateway-clients'(["lookup", Name, ClientId]) ->
    ChanTab = emqx_gateway_cm:tabname(chan, Name),
    case ets:info(ChanTab) of
        undefined ->
            print("Bad Gateway Name.\n");
        _ ->
            case ets:lookup(ChanTab, bin(ClientId)) of
                [] ->
                    print("Not Found.\n");
                [Chann] ->
                    InfoTab = emqx_gateway_cm:tabname(info, Name),
                    [ChannInfo] = ets:lookup(InfoTab, Chann),
                    print_record({client, ChannInfo})
            end
    end;
'gateway-clients'(["kick", Name, ClientId]) ->
    case emqx_gateway_cm:kick_session(Name, bin(ClientId)) of
        ok -> print("ok\n");
        _ -> print("Not Found.\n")
    end;
'gateway-clients'(_) ->
    emqx_ctl:usage([
        {"gateway-clients list   <Name>", "List all clients for a gateway"},
        {"gateway-clients lookup <Name> <ClientId>", "Lookup the Client Info for specified client"},
        {"gateway-clients kick   <Name> <ClientId>", "Kick out a client"}
    ]).

'gateway-metrics'([Name]) ->
    case emqx_gateway_metrics:lookup(atom(Name)) of
        undefined ->
            print("Bad Gateway Name.\n");
        Metrics ->
            lists:foreach(
                fun({K, V}) -> print("~-30s: ~w\n", [K, V]) end,
                Metrics
            )
    end;
'gateway-metrics'(_) ->
    emqx_ctl:usage([{"gateway-metrics <Name>", "List all metrics for a gateway"}]).

atom(Id) ->
    try
        list_to_existing_atom(Id)
    catch
        _:_ -> undefined
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
    PrintValue = [print_record({Tag, Record}) || Record <- ets:lookup(Table, Key)],
    dump(Table, Tag, ets:next(Table, Key), [PrintValue | Result]).

print_record({client, {_, Infos, Stats}}) ->
    ClientInfo = maps:get(clientinfo, Infos, #{}),
    ConnInfo = maps:get(conninfo, Infos, #{}),
    _Session = maps:get(session, Infos, #{}),
    SafeGet = fun(K, M) -> maps:get(K, M, undefined) end,
    StatsGet = fun(K) -> proplists:get_value(K, Stats, 0) end,

    ConnectedAt = SafeGet(connected_at, ConnInfo),
    InfoKeys = [
        clientid,
        username,
        peername,
        clean_start,
        keepalive,
        subscriptions_cnt,
        send_msg,
        connected,
        created_at,
        connected_at
    ],
    Info = #{
        clientid => SafeGet(clientid, ClientInfo),
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

    print(
        "Client(~ts, username=~ts, peername=~ts, "
        "clean_start=~ts, keepalive=~w, "
        "subscriptions=~w, delivered_msgs=~w, "
        "connected=~ts, created_at=~w, connected_at=~w)\n",
        [format(K, maps:get(K, Info)) || K <- InfoKeys]
    ).

print(S) -> emqx_ctl:print(S).
print(S, A) -> emqx_ctl:print(S, A).

format(_, undefined) ->
    undefined;
format(peername, {IPAddr, Port}) ->
    IPStr = emqx_mgmt_util:ntoa(IPAddr),
    io_lib:format("~ts:~p", [IPStr, Port]);
format(_, Val) ->
    Val.

format_gw_summary(#{name := Name, status := unloaded}) ->
    io_lib:format("Gateway(name=~ts, status=unloaded)\n", [Name]);
format_gw_summary(#{
    name := Name,
    status := stopped,
    stopped_at := StoppedAt
}) ->
    io_lib:format(
        "Gateway(name=~ts, status=stopped, stopped_at=~ts)\n",
        [Name, StoppedAt]
    );
format_gw_summary(#{
    name := Name,
    status := running,
    current_connections := ConnCnt,
    started_at := StartedAt
}) ->
    io_lib:format(
        "Gateway(name=~ts, status=running, clients=~w, "
        "started_at=~ts)\n",
        [Name, ConnCnt, StartedAt]
    ).

format_gateway(#{
    name := Name,
    status := unloaded
}) ->
    io_lib:format(
        "name: ~ts\n"
        "status: unloaded\n",
        [Name]
    );
format_gateway(
    Gw =
        #{
            name := Name,
            status := Status,
            created_at := CreatedAt,
            config := Config
        }
) ->
    {StopOrStart, Timestamp} =
        case Status of
            stopped -> {stopped_at, maps:get(stopped_at, Gw)};
            running -> {started_at, maps:get(started_at, Gw)}
        end,
    io_lib:format(
        "name: ~ts\n"
        "status: ~ts\n"
        "created_at: ~ts\n"
        "~ts: ~ts\n"
        "config: ~p\n",
        [
            Name,
            Status,
            emqx_gateway_utils:unix_ts_to_rfc3339(CreatedAt),
            StopOrStart,
            emqx_gateway_utils:unix_ts_to_rfc3339(Timestamp),
            Config
        ]
    ).

format_error(Reason) ->
    case emqx_gateway_http:reason2msg(Reason) of
        error -> io_lib:format("~p", [Reason]);
        Msg -> Msg
    end.
