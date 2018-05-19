%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Enterprise, Inc. (http://emqtt.io)
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
-http_api({"^clients/(.+?)/?$", 'DELETE', kick_client, []}).
-http_api({"^clients/(.+?)/clean_acl_cache?$", 'PUT', clean_acl_cache, [{<<"topic">>, binary}]}).

-http_api({"^routes?$", 'GET', route_list, []}).
-http_api({"^routes/(.+?)/?$", 'GET', route, []}).

-http_api({"^nodes/(.+?)/sessions/?$", 'GET', session_list, []}).
-http_api({"^nodes/(.+?)/sessions/(.+?)/?$", 'GET', session_list, []}).
-http_api({"^sessions/(.+?)/?$", 'GET', session, []}).

-http_api({"^nodes/(.+?)/subscriptions/?$", 'GET', subscription_list, []}).
-http_api({"^nodes/(.+?)/subscriptions/(.+?)/?$", 'GET', subscription_list, []}).
-http_api({"^subscriptions/(.+?)/?$", 'GET', subscription, []}).

-http_api({"^mqtt/publish?$", 'POST', publish, [{<<"topic">>, binary}, {<<"payload">>, binary}]}).
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

-http_api({"^configs/(.+?)/?$", 'PUT', modify_config, [{<<"key">>, binary}, {<<"value">>, binary}]}).
-http_api({"^configs/?$", 'GET', config_list, []}).
-http_api({"^nodes/(.+?)/configs/(.+?)/?$", 'PUT', modify_config, [{<<"key">>, binary}, {<<"value">>, binary}]}).
-http_api({"^nodes/(.+?)/configs/?$", 'GET', config_list, []}).
-http_api({"^nodes/(.+?)/plugin_configs/(.+?)/?$", 'GET', plugin_config_list, []}).
-http_api({"^nodes/(.+?)/plugin_configs/(.+?)/?$", 'PUT', modify_plugin_config, []}).

-http_api({"^users/?$", 'GET', users, []}).
-http_api({"^users/?$", 'POST', users, [{<<"username">>, binary},
                                        {<<"password">>, binary},
                                        {<<"tags">>, binary}]}).
-http_api({"^users/(.+?)/?$", 'GET', users, []}).
-http_api({"^users/(.+?)/?$", 'PUT', users, [{<<"tags">>, binary}]}).
-http_api({"^users/(.+?)/?$", 'DELETE', users, []}).

-http_api({"^auth/?$", 'POST', auth, [{<<"username">>, binary}, {<<"password">>, binary}]}).
-http_api({"^change_pwd/(.+?)/?$", 'PUT', change_pwd, [{<<"old_pwd">>, binary},
                                                       {<<"new_pwd">>, binary}]}).

-import(proplists, [get_value/2, get_value/3]).

-export([alarm_list/3]).
-export([client/3, client_list/3, client_list/4, kick_client/3, clean_acl_cache/3]).
-export([route/3, route_list/2]).
-export([session/3, session_list/3, session_list/4]).
-export([subscription/3, subscription_list/3, subscription_list/4]).
-export([nodes/2, node/3, brokers/2, broker/3, listeners/2, listener/3, metrics/2, metric/3, stats/2, stat/3]).
-export([publish/2, subscribe/2, unsubscribe/2]).
-export([plugin_list/3, enabled/4]).
-export([modify_config/3, modify_config/4, config_list/2, config_list/3,
         plugin_config_list/4, modify_plugin_config/4]).

-export([users/2,users/3, auth/2, change_pwd/3]).

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
    Rows = get_value(result, Data),
            TotalPage = get_value(totalPage, Data),
            TotalNum  = get_value(totalNum, Data),
            {ok, [{current_page, PageNo}, 
                  {page_size, PageSize},
                  {total_num, TotalNum},
                  {total_page, TotalPage},
                  {objects, [client_row(Row) || Row <- Rows]}]}.

client_list('GET', Params, Node, Key) ->
    {PageNo, PageSize} = page_params(Params),
    Data = emqttd_mgmt:client_list(l2a(Node), l2b(Key), PageNo, PageSize),
    {ok, [{objects, [client_row(Row) || Row <- Data]}]}.

kick_client('DELETE', _Params, Key) ->
    case emqttd_mgmt:kick_client(l2b(Key)) of
        true  -> {ok, []};
        false -> {error, [{code, ?ERROR12}]}
    end.

clean_acl_cache('PUT', Params, Key0) ->
    Topic = get_value(<<"topic">>, Params),
    [Key | _] = string:tokens(Key0, "/"),
    case emqttd_mgmt:clean_acl_cache(l2b(Key), Topic) of
        true  -> {ok, []};
        false -> {error, [{code, ?ERROR12}]}
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
    Rows = get_value(result, Data),
    TotalPage = get_value(totalPage, Data),
    TotalNum  = get_value(totalNum, Data),
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
    Rows = get_value(result, Data),
    TotalPage = get_value(totalPage, Data),
    TotalNum  = get_value(totalNum, Data),
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
    Data = lists:append(Session, emqttd_stats:get_session_stats(ClientId)),
    InfoKeys = [clean_sess, subscriptions, max_inflight, inflight_len, mqueue_len,
                mqueue_dropped, awaiting_rel_len, deliver_msg,enqueue_msg, created_at],
    [{client_id, ClientId} | [{Key, format(Key, get_value(Key, Data))} || Key <- InfoKeys]].

%%--------------------------------------------------------------------------
%% subscription
%%--------------------------------------------------------------------------
subscription('GET', _Params, Key) ->
    Data = emqttd_mgmt:subscription(l2b(Key)),
    {ok, [{objects, [subscription_row(Row) || Row <- Data]}]}.

subscription_list('GET', Params, Node) ->
    {PageNo, PageSize} = page_params(Params),
    Data = emqttd_mgmt:subscription_list(l2a(Node), undefined, PageNo, PageSize),
    Rows = get_value(result, Data),
    TotalPage = get_value(totalPage, Data),
    TotalNum  = get_value(totalNum, Data),
    {ok, [{current_page, PageNo}, 
          {page_size, PageSize},
          {total_num, TotalNum},
          {total_page, TotalPage},
          {objects, [subscription_row(Row) || Row <- Rows]}]}.

subscription_list('GET', Params, Node, Key) ->
    {PageNo, PageSize} = page_params(Params),
    Data = emqttd_mgmt:subscription_list(l2a(Node), l2b(Key), PageNo, PageSize),
    {ok, [{objects, [subscription_row(Row) || Row <- Data]}]}.

subscription_row({{Topic, SubPid}, Options}) when is_pid(SubPid) ->
    subscription_row({{Topic, {undefined, SubPid}}, Options});
subscription_row({{Topic, {SubId, SubPid}}, Options}) ->
    Qos = proplists:get_value(qos, Options),
    ClientId = case SubId of
                   undefined -> list_to_binary(pid_to_list(SubPid));
                   SubId     -> SubId
               end,
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
    {ok, [[{Node, format_listeners(Listeners, [])} || {Node, Listeners} <- Data]]}.

listener('GET', _Params, Node) ->
    Data = emqttd_mgmt:listener(l2a(Node)),
    {ok, [format_listener(Listeners) || Listeners <- Data]}.

metrics('GET', _Params) ->
    Data = emqttd_mgmt:metrics(),
    {ok, [Data]}.

metric('GET', _Params, Node) ->
    Data = emqttd_mgmt:metrics(l2a(Node)),
    {ok, Data}.

stats('GET', _Params) ->
    Data = emqttd_mgmt:stats(),
    {ok, [Data]}.

stat('GET', _Params, Node) ->
    Data = emqttd_mgmt:stats(l2a(Node)),
    {ok, Data}.

format_broker(Node, Broker) ->
    OtpRel  = "R" ++ erlang:system_info(otp_release) ++ "/" ++ erlang:system_info(version),
    [{name,     Node},
     {version,  bin(get_value(version, Broker))},
     {sysdescr, bin(get_value(sysdescr, Broker))},
     {uptime,   bin(get_value(uptime, Broker))},
     {datetime, bin(get_value(datetime, Broker))},
     {otp_release, l2b(OtpRel)},
     {node_status, 'Running'}].

format_broker(Broker) ->
    OtpRel  = "R" ++ erlang:system_info(otp_release) ++ "/" ++ erlang:system_info(version),
    [{version,  bin(get_value(version, Broker))},
     {sysdescr, bin(get_value(sysdescr, Broker))},
     {uptime,   bin(get_value(uptime, Broker))},
     {datetime, bin(get_value(datetime, Broker))},
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
    Topic = get_value(<<"topic">>, Params),
    ClientId = get_value(<<"client_id">>, Params, http),
    Payload = get_value(<<"payload">>, Params, <<>>),
    Qos     = get_value(<<"qos">>, Params, 0),
    Retain  = get_value(<<"retain">>, Params, false),
    case emqttd_mgmt:publish({ClientId, Topic, Payload, Qos, Retain}) of
        ok ->
            {ok, []};
        {error, Error} ->
            {error, [{code, ?ERROR2}, {message, Error}]}
    end.

subscribe('POST', Params) ->
    ClientId = get_value(<<"client_id">>, Params),
    Topic    = get_value(<<"topic">>, Params),
    Qos      = get_value(<<"qos">>, Params, 0),
    case emqttd_mgmt:subscribe({ClientId, Topic, Qos}) of
        ok ->
            {ok, []};
        {error, Error} ->
            {error, [{code, ?ERROR2}, {message, Error}]}
    end.

unsubscribe('POST', Params) ->
    ClientId = get_value(<<"client_id">>, Params),
    Topic    = get_value(<<"topic">>, Params),
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
    Active = get_value(<<"active">>, Params),
    case Active of
        true ->
            return(emqttd_mgmt:plugin_load(l2a(Node), l2a(PluginName)));
        false ->
            return(emqttd_mgmt:plugin_unload(l2a(Node), l2a(PluginName)))
    end.

return(Result) ->
    case Result of
        ok ->
            {ok, []};
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
%% modify config
%%--------------------------------------------------------------------------
modify_config('PUT', Params, App) ->
    Key   = get_value(<<"key">>, Params, <<"">>),
    Value = get_value(<<"value">>, Params, <<"">>),
    case emqttd_mgmt:modify_config(l2a(App), b2l(Key), b2l(Value)) of
        true  -> {ok, []};
        false -> {error, [{code, ?ERROR2}]}
    end.

modify_config('PUT', Params, Node, App) ->
    Key   = get_value(<<"key">>, Params, <<"">>),
    Value = get_value(<<"value">>, Params, <<"">>),
    case emqttd_mgmt:modify_config(l2a(Node), l2a(App), b2l(Key), b2l(Value)) of
        ok  -> {ok, []};
        _ -> {error, [{code, ?ERROR2}]}
    end.

config_list('GET', _Params) ->
    Data = emqttd_mgmt:get_configs(),
    {ok, [{Node, format_config(Config, [])} || {Node, Config} <- Data]}.

config_list('GET', _Params, Node) ->
    Data = emqttd_mgmt:get_config(l2a(Node)),
    {ok, [format_config(Config) || Config <- lists:reverse(Data)]}.

plugin_config_list('GET', _Params, Node, App) ->
    {ok, Data} = emqttd_mgmt:get_plugin_config(l2a(Node), l2a(App)),
    {ok, [format_plugin_config(Config) || Config <- lists:reverse(Data)]}.

modify_plugin_config('PUT', Params, Node, App) ->
    PluginName = l2a(App),
    case emqttd_mgmt:modify_plugin_config(l2a(Node), PluginName, Params) of
        ok  ->
            Plugins = emqttd_plugins:list(),
            {_, _, _, _, Status} = lists:keyfind(PluginName, 2, Plugins),
            case Status of
                true  ->
                    emqttd_plugins:unload(PluginName),
                    timer:sleep(500),
                    emqttd_plugins:load(PluginName),
                    {ok, []};
                false ->
                    {ok, []}
            end;
        _ ->
            {error, [{code, ?ERROR2}]}
    end.


format_config([], Acc) ->
    Acc;
format_config([{Key, Value, Datatpye, App}| Configs], Acc) ->
    format_config(Configs, [format_config({Key, Value, Datatpye, App}) | Acc]).

format_config({Key, Value, Datatpye, App}) ->
    [{<<"key">>, l2b(Key)},
     {<<"value">>, l2b(Value)},
     {<<"datatpye">>, l2b(Datatpye)},
     {<<"app">>, App}].

format_plugin_config({Key, Value, Desc, Required}) ->
    [{<<"key">>, l2b(Key)},
     {<<"value">>, l2b(Value)},
     {<<"desc">>, l2b(Desc)},
     {<<"required">>, Required}].

%%--------------------------------------------------------------------------
%% Admin
%%--------------------------------------------------------------------------
auth('POST', Params) ->
    Username = get_value(<<"username">>, Params),
    Password = get_value(<<"password">>, Params),
    case emqttd_mgmt:check_user(Username, Password) of
        ok ->
            {ok, []};
        {error, Reason} ->
            {error, [{code, ?ERROR3}, {message, list_to_binary(Reason)}]}
    end.

users('POST', Params) ->
    Username = get_value(<<"username">>, Params),
    Password = get_value(<<"password">>, Params),
    Tag = get_value(<<"tags">>, Params),
    code(emqttd_mgmt:add_user(Username, Password, Tag));

users('GET', _Params) ->
    {ok, [Admin || Admin <- emqttd_mgmt:user_list()]}.

users('GET', _Params, Username) ->
    {ok, emqttd_mgmt:lookup_user(list_to_binary(Username))};

users('PUT', Params, Username) ->
    code(emqttd_mgmt:update_user(list_to_binary(Username), Params));

users('DELETE', _Params, "admin") ->
    {error, [{code, ?ERROR6}, {message, <<"admin cannot be deleted">>}]};
users('DELETE', _Params, Username) ->
    code(emqttd_mgmt:remove_user(list_to_binary(Username))).

change_pwd('PUT', Params, Username) ->
    OldPwd = get_value(<<"old_pwd">>, Params),
    NewPwd = get_value(<<"new_pwd">>, Params),
    code(emqttd_mgmt:change_password(list_to_binary(Username), OldPwd, NewPwd)).

code(ok)             -> {ok, []};
code(error)          -> {error, [{code, ?ERROR2}]};
code({error, Error}) -> {error, Error}.
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
b2l(B) -> binary_to_list(B).


page_params(Params) ->
    PageNo = int(get_value("curr_page", Params, "1")),
    PageSize = int(get_value("page_size", Params, "20")),
    {PageNo, PageSize}.
