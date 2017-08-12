%%--------------------------------------------------------------------
%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
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
-module (emqttd_rest_api).

-include("emqttd.hrl").

-include("emqttd_internal.hrl").

-http_api({"^nodes/(.+?)/alarms/?$", 'GET', alarm_list, []}).

-http_api({"^nodes/(.+?)/clients/?$", 'GET', client_list, []}).
-http_api({"^nodes/(.+?)/clients/(.+?)/?$", 'GET',client_list, []}).
-http_api({"^clients/(.+?)/?$", 'GET', client, []}).
-http_api({"^kick_client/(.+?)/?$", 'PUT', kick_client, []}).

-http_api({"^routes?$", 'GET', route_list, []}).
-http_api({"^routes/(.+?)/?$", 'GET', route, []}).

-http_api({"^nodes/(.+?)/sessions/?$", 'GET', session_list, []}).
-http_api({"^nodes/(.+?)/sessions/(.+?)/?$", 'GET', session_list, []}).
-http_api({"^sessions/(.+?)/?$", 'GET', session, []}).

-http_api({"^nodes/(.+?)/subscriptions/?$", 'GET', subscription_list, []}).
-http_api({"^nodes/(.+?)/subscriptions/(.+?)/?$", 'GET', subscription_list, []}).
-http_api({"^subscriptions/(.+?)/?$", 'GET', subscription, []}).

-http_api({"^mqtt/publish?$", 'POST', publish, [{<<"topic">>, binary}]}).
-http_api({"^mqtt/subscribe?$", 'POST', subscribe, [{<<"client_id">>, binary},{<<"topic">>, binary}]}).
-http_api({"^mqtt/unsubscribe?$", 'POST', unsubscribe, [{<<"client_id">>, binary},{<<"topic">>, binary}]}).

-http_api({"^management/nodes/?$", 'GET', brokers, []}).
-http_api({"^management/nodes/(.+?)/?$", 'GET', broker, []}).
-http_api({"^monitoring/nodes/?$", 'GET', nodes, []}).
-http_api({"^monitoring/nodes/(.+?)/?$", 'GET', node, []}).
-http_api({"^monitoring/listeners/?$", 'GET', listeners, []}).
-http_api({"^monitoring/listeners/(.+?)/?$", 'GET', listener, []}).
-http_api({"^monitoring/metrics/?$", 'GET', metrics, []}).
-http_api({"^monitoring/metrics/(.+?)/?$", 'GET', metric, []}).
-http_api({"^monitoring/stats/?$", 'GET', stats, []}).
-http_api({"^monitoring/stats/(.+?)/?$", 'GET', stat, []}).

-http_api({"^nodes/(.+?)/plugins/?$", 'GET', plugin_list, []}).
-http_api({"^nodes/(.+?)/plugins/(.+?)/?$", 'PUT', enabled, [{<<"active">>, bool}]}).

-export([alarm_list/3]).
-export([client/3, client_list/3, client_list/4, kick_client/3]).
-export([route/3, route_list/2]).
-export([session/3, session_list/3, session_list/4]).
-export([subscription/3, subscription_list/3, subscription_list/4]).
-export([nodes/2, node/3, brokers/2, broker/3, listeners/2, listener/3, metrics/2, metric/3, stats/2, stat/3]).
-export([publish/2, subscribe/2, unsubscribe/2]).
-export([plugin_list/3, enabled/4]).

%%--------------------------------------------------------------------------
%% alarm
%%--------------------------------------------------------------------------
alarm_list('GET', _Req, _Node) ->
    Alarms = emqttd_mgmt:alarm_list(),
    {ok, lists:map(fun alarm_row/1, Alarms)}.

alarm_row(#mqtt_alarm{id        = AlarmId,
                      severity  = Severity,
                      title     = Title,
                      summary   = Summary,
                      timestamp = Timestamp}) ->
    [{id, AlarmId},
     {severity, Severity},
     {title, l2b(Title)},
     {summary, l2b(Summary)},
     {occurred_at, l2b(strftime(Timestamp))}].

%%--------------------------------------------------------------------------
%% client
%%--------------------------------------------------------------------------
client('GET', _Params, Key) ->
    Data = emqttd_mgmt:client(l2b(Key)),
    {ok, [{objects, [client_row(Row) || Row <- Data]}]}.

client_list('GET', Params, Node) ->
    {PageNo, PageSize} = page_params(Params),
    Data = emqttd_mgmt:client_list(l2a(Node), undefined, PageNo, PageSize),
    Rows = proplists:get_value(result, Data),
            TotalPage = proplists:get_value(totalPage, Data),
            TotalNum  = proplists:get_value(totalNum, Data),
            {ok, [{current_page, PageNo}, 
                  {page_size, PageSize},
                  {total_num, TotalNum},
                  {total_page, TotalPage},
                  {objects, [client_row(Row) || Row <- Rows]}]}.

client_list('GET', Params, Node, Key) ->
    {PageNo, PageSize} = page_params(Params),
    Data = emqttd_mgmt:client_list(l2a(Node), l2b(Key), PageNo, PageSize),
    {ok, [{objects, [client_row(Row) || Row <- Data]}]}.

kick_client('PUT', _Params, Key) ->
    case emqttd_mgmt:kick_client(l2b(Key)) of
        ok -> {ok, []};
        error -> {error, [{code, ?ERROR12}]}
    end.

client_row(#mqtt_client{client_id = ClientId,
                 peername = {IpAddr, Port},
                 username = Username,
                 clean_sess = CleanSess,
                 proto_ver = ProtoVer,
                 keepalive = KeepAlvie,
                 connected_at = ConnectedAt}) ->
    [{client_id, ClientId},
     {username, Username},
     {ipaddress, l2b(ntoa(IpAddr))},
     {port, Port},
     {clean_sess, CleanSess},
     {proto_ver, ProtoVer},
     {keepalive, KeepAlvie},
     {connected_at, l2b(strftime(ConnectedAt))}].

%%--------------------------------------------------------------------------
%% route
%%--------------------------------------------------------------------------
route('GET', _Params, Key) ->
    Data = emqttd_mgmt:route(l2b(Key)),
    {ok, [{objects, [route_row(Row) || Row <- Data]}]}.

route_list('GET', Params) ->
    {PageNo, PageSize} = page_params(Params),
    Data = emqttd_mgmt:route_list(undefined, PageNo, PageSize),
    Rows = proplists:get_value(result, Data),
    TotalPage = proplists:get_value(totalPage, Data),
    TotalNum  = proplists:get_value(totalNum, Data),
    {ok, [{current_page, PageNo}, 
          {page_size, PageSize},
          {total_num, TotalNum},
          {total_page, TotalPage},
          {objects, [route_row(Row) || Row <- Rows]}]}.

route_row(Route) when is_record(Route, mqtt_route) ->
    [{topic, Route#mqtt_route.topic}, {node, Route#mqtt_route.node}];

route_row({Topic, Node}) ->
    [{topic, Topic}, {node, Node}].

%%--------------------------------------------------------------------------
%% session
%%--------------------------------------------------------------------------
session('GET', _Params, Key) ->
    Data = emqttd_mgmt:session(l2b(Key)),
    {ok, [{objects, [session_row(Row) || Row <- Data]}]}.

session_list('GET', Params, Node) ->
    {PageNo, PageSize} = page_params(Params),
    Data = emqttd_mgmt:session_list(l2a(Node), undefined, PageNo, PageSize),
    Rows = proplists:get_value(result, Data),
    TotalPage = proplists:get_value(totalPage, Data),
    TotalNum  = proplists:get_value(totalNum, Data),
    {ok, [{current_page, PageNo}, 
          {page_size, PageSize},
          {total_num, TotalNum},
          {total_page, TotalPage},
          {objects, [session_row(Row) || Row <- Rows]}]}.

session_list('GET', Params, Node, ClientId) ->
    {PageNo, PageSize} = page_params(Params),
    Data = emqttd_mgmt:session_list(l2a(Node), l2b(ClientId), PageNo, PageSize),
    {ok, [{objects, [session_row(Row) || Row <- Data]}]}.

session_row({ClientId, _Pid, _Persistent, Session}) ->
    InfoKeys = [clean_sess, max_inflight, inflight_queue, message_queue,
                message_dropped, awaiting_rel, awaiting_ack, awaiting_comp, created_at],
     [{client_id, ClientId} | [{Key, format(Key, proplists:get_value(Key, Session))} || Key <- InfoKeys]].

%%--------------------------------------------------------------------------
%% subscription
%%--------------------------------------------------------------------------
subscription('GET', _Params, Key) ->
    Data = emqttd_mgmt:subscription(l2b(Key)),
    {ok, [{objects, [subscription_row(Row) || Row <- Data]}]}.

subscription_list('GET', Params, Node) ->
    {PageNo, PageSize} = page_params(Params),
    Data = emqttd_mgmt:subscription_list(l2a(Node), undefined, PageNo, PageSize),
    Rows = proplists:get_value(result, Data),
    TotalPage = proplists:get_value(totalPage, Data),
    TotalNum  = proplists:get_value(totalNum, Data),
    {ok, [{current_page, PageNo}, 
          {page_size, PageSize},
          {total_num, TotalNum},
          {total_page, TotalPage},
          {objects, [subscription_row(Row) || Row <- Rows]}]}.

subscription_list('GET', Params, Node, Key) ->
    {PageNo, PageSize} = page_params(Params),
    Data = emqttd_mgmt:subscription_list(l2a(Node), l2b(Key), PageNo, PageSize),
    {ok, [{objects, [subscription_row(Row) || Row <- Data]}]}.

subscription_row({{Topic, ClientId}, Option}) when is_pid(ClientId) ->
    subscription_row({{Topic, l2b(pid_to_list(ClientId))}, Option});    
subscription_row({{Topic, ClientId}, Option}) ->
    Qos = proplists:get_value(qos, Option),
    [{client_id, ClientId}, {topic, Topic}, {qos, Qos}].

%%--------------------------------------------------------------------------
%% management/monitoring
%%--------------------------------------------------------------------------
nodes('GET', _Params) ->
    Data = emqttd_mgmt:nodes_info(),
    {ok, Data}.

node('GET', _Params, Node) ->
    Data = emqttd_mgmt:node_info(l2a(Node)),
    {ok, Data}.

brokers('GET', _Params) ->
    Data = emqttd_mgmt:brokers(),
    {ok, [format_broker(Node, Broker) || {Node, Broker} <- Data]}.

broker('GET', _Params, Node) ->
    Data = emqttd_mgmt:broker(l2a(Node)),
    {ok, format_broker(Data)}.

listeners('GET', _Params) ->
    Data = emqttd_mgmt:listeners(),
    {ok, [{Node, format_listeners(Listeners, [])} || {Node, Listeners} <- Data]}.

listener('GET', _Params, Node) ->
    Data = emqttd_mgmt:listener(l2a(Node)),
    {ok, [format_listener(Listeners) || Listeners <- Data]}.

metrics('GET', _Params) ->
    Data = emqttd_mgmt:metrics(),
    {ok, Data}.

metric('GET', _Params, Node) ->
    Data = emqttd_mgmt:metrics(l2a(Node)),
    {ok, Data}.

stats('GET', _Params) ->
    Data = emqttd_mgmt:stats(),
    {ok, Data}.

stat('GET', _Params, Node) ->
    Data = emqttd_mgmt:stats(l2a(Node)),
    {ok, Data}.

format_broker(Node, Broker) ->
    OtpRel  = "R" ++ erlang:system_info(otp_release) ++ "/" ++ erlang:system_info(version),
    [{name,     Node},
     {version,  bin(proplists:get_value(version, Broker))},
     {sysdescr, bin(proplists:get_value(sysdescr, Broker))},
     {uptime,   bin(proplists:get_value(uptime, Broker))},
     {datetime, bin(proplists:get_value(datetime, Broker))},
     {otp_release, l2b(OtpRel)},
     {node_status, 'Running'}].

format_broker(Broker) ->
    OtpRel  = "R" ++ erlang:system_info(otp_release) ++ "/" ++ erlang:system_info(version),
    [{version,  bin(proplists:get_value(version, Broker))},
     {sysdescr, bin(proplists:get_value(sysdescr, Broker))},
     {uptime,   bin(proplists:get_value(uptime, Broker))},
     {datetime, bin(proplists:get_value(datetime, Broker))},
     {otp_release, l2b(OtpRel)},
     {node_status, 'Running'}].

format_listeners([], Acc) ->
    Acc;
format_listeners([{Protocol, ListenOn, Info}| Listeners], Acc) ->
    format_listeners(Listeners, [format_listener({Protocol, ListenOn, Info}) | Acc]).

format_listener({Protocol, ListenOn, Info}) ->
    Listen = l2b(esockd:to_string(ListenOn)),
    lists:append([{protocol, Protocol}, {listen, Listen}], Info).

%%--------------------------------------------------------------------------
%% mqtt
%%--------------------------------------------------------------------------
publish('POST', Params) ->
    Topic = proplists:get_value(<<"topic">>, Params),
    ClientId = proplists:get_value(<<"client_id">>, Params, http),
    Payload = proplists:get_value(<<"payload">>, Params, <<>>),
    Qos     = proplists:get_value(<<"qos">>, Params, 0),
    Retain  = proplists:get_value(<<"retain">>, Params, false),
    case emqttd_mgmt:publish({ClientId, Topic, Payload, Qos, Retain}) of
        ok ->
            {ok, []};
        {error, Error} ->
            {error, [{code, ?ERROR2}, {message, Error}]}
    end.

subscribe('POST', Params) ->
    ClientId = proplists:get_value(<<"client_id">>, Params),
    Topic    = proplists:get_value(<<"topic">>, Params),
    Qos      = proplists:get_value(<<"qos">>, Params, 0),
    case emqttd_mgmt:subscribe({ClientId, Topic, Qos}) of
        ok ->
            {ok, []};
        {error, Error} ->
            {error, [{code, ?ERROR2}, {message, Error}]}
    end.

unsubscribe('POST', Params) ->
    ClientId = proplists:get_value(<<"client_id">>, Params),
    Topic    = proplists:get_value(<<"topic">>, Params),
    case emqttd_mgmt:unsubscribe({ClientId, Topic})of
        ok ->
            {ok, []};
        {error, Error} ->
            {error, [{code, ?ERROR2}, {message, Error}]}
    end.

%%--------------------------------------------------------------------------
%% plugins
%%--------------------------------------------------------------------------
plugin_list('GET', _Params, Node) ->
    Plugins = lists:map(fun plugin/1, emqttd_mgmt:plugin_list(l2a(Node))),
    {ok, Plugins}.

enabled('PUT', Params, Node, PluginName) ->
    Active = proplists:get_value(<<"active">>, Params),
    case Active of
        true ->
            return(emqttd_mgmt:plugin_load(l2a(Node), l2a(PluginName)));
        false ->
            return(emqttd_mgmt:plugin_unload(l2a(Node), l2a(PluginName)))
    end.

return(Result) ->
    case Result of
        {ok, _} ->
            {ok, []};
        {error, already_started} ->
            {error, [{code, ?ERROR10}, {message, <<"already_started">>}]};
        {error, not_started} ->
            {error, [{code, ?ERROR11}, {message, <<"not_started">>}]};
        Error ->
            lager:error("error:~p", [Error]),
            {error, [{code, ?ERROR2}, {message, <<"unknown">>}]}
    end.
plugin(#mqtt_plugin{name = Name, version = Ver, descr = Descr,
                    active = Active}) ->
    [{name, Name},
     {version, iolist_to_binary(Ver)},
     {description, iolist_to_binary(Descr)},
     {active, Active}].

%%--------------------------------------------------------------------------
%% Inner function
%%--------------------------------------------------------------------------
format(created_at, Val) ->
    l2b(strftime(Val));
format(_, Val) ->
    Val.

ntoa({0,0,0,0,0,16#ffff,AB,CD}) ->
    inet_parse:ntoa({AB bsr 8, AB rem 256, CD bsr 8, CD rem 256});
ntoa(IP) ->
    inet_parse:ntoa(IP).

%%--------------------------------------------------------------------
%% Strftime
%%--------------------------------------------------------------------
strftime({MegaSecs, Secs, _MicroSecs}) ->
    strftime(datetime(MegaSecs * 1000000 + Secs));

strftime({{Y,M,D}, {H,MM,S}}) ->
    lists:flatten(
        io_lib:format(
            "~4..0w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w", [Y, M, D, H, MM, S])).

datetime(Timestamp) when is_integer(Timestamp) ->
    Universal = calendar:gregorian_seconds_to_datetime(Timestamp +
    calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}})),
    calendar:universal_time_to_local_time(Universal).

bin(S) when is_list(S)   -> l2b(S);
bin(A) when is_atom(A)   -> bin(atom_to_list(A));
bin(B) when is_binary(B) -> B;
bin(undefined) -> <<>>.
int(L) -> list_to_integer(L).
l2a(L) -> list_to_atom(L).
l2b(L) -> list_to_binary(L).


page_params(Params) ->
    PageNo = int(proplists:get_value("curr_page", Params, "1")),
    PageSize = int(proplists:get_value("page_size", Params, "20")),
    {PageNo, PageSize}.