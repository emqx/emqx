%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_sys).

-behaviour(gen_server).

-include("emqx.hrl").
-include("types.hrl").
-include("logger.hrl").
-include("emqx_hooks.hrl").
-include("emqx_mqtt.hrl").

-export([
    start_link/0,
    stop/0
]).

-export([
    version/0,
    cluster_name/0,
    uptime/0,
    datetime/0,
    sysdescr/0
]).

-export([info/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-export([
    on_client_connected/2,
    on_client_disconnected/3,
    on_client_subscribed/3,
    on_client_unsubscribed/3
]).

-export([post_config_update/5]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-import(emqx_topic, [systop/1]).
-import(emqx_utils, [start_timer/2]).

-record(state, {
    heartbeat :: option(reference()),
    ticker :: option(reference()),
    sysdescr :: binary()
}).

-define(APP, emqx).
-define(SYS, ?MODULE).

-define(INFO_KEYS,
    % Broker version
    [
        version,
        % Broker uptime
        uptime,
        % Broker local datetime
        datetime,
        % Broker description
        sysdescr
    ]
).

-define(CONF_KEY_PATH, [sys_topics]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec start_link() -> {ok, pid()} | ignore | {error, any()}.
start_link() ->
    gen_server:start_link({local, ?SYS}, ?MODULE, [], []).

stop() ->
    gen_server:stop(?SYS).

%% @doc Get sys version
-spec version() -> string().
version() -> emqx_app:get_release().

%% @doc Get cluster name
-spec cluster_name() -> string().
cluster_name() -> atom_to_list(ekka:cluster_name()).

%% @doc Get sys description
-spec sysdescr() -> string().
sysdescr() -> emqx_app:get_description().

%% @doc Get sys uptime
-spec uptime() -> Milliseconds :: integer().
uptime() ->
    {TotalWallClock, _} = erlang:statistics(wall_clock),
    TotalWallClock.

%% @doc Get sys datetime
-spec datetime() -> string().
datetime() ->
    calendar:system_time_to_rfc3339(erlang:system_time(), [{unit, nanosecond}]).

sys_interval() ->
    emqx:get_config([sys_topics, sys_msg_interval]).

sys_heatbeat_interval() ->
    emqx:get_config([sys_topics, sys_heartbeat_interval]).

sys_event_messages() ->
    maps:to_list(emqx:get_config([sys_topics, sys_event_messages])).

%% @doc Get sys info
-spec info() -> list(tuple()).
info() ->
    [
        {version, version()},
        {sysdescr, sysdescr()},
        {uptime, uptime()},
        {datetime, datetime()}
    ].

%% Update the confgs at runtime
post_config_update(_, _Req, NewSysConf, OldSysConf, _AppEnvs) ->
    {Added, Removed} = diff_hooks(NewSysConf, OldSysConf),
    unload_event_hooks(Removed),
    load_event_hooks(Added).

diff_hooks(NewSysConf, OldSysConf) ->
    NewEvents = maps:to_list(maps:get(sys_event_messages, NewSysConf, #{})),
    OldEvents = maps:to_list(maps:get(sys_event_messages, OldSysConf, #{})),
    diff_hooks(NewEvents, OldEvents, [], []).

diff_hooks([], [], Added, Removed) ->
    {lists:reverse(Added), lists:reverse(Removed)};
diff_hooks([H | T1], [H | T2], Added, Removed) ->
    diff_hooks(T1, T2, Added, Removed);
diff_hooks(
    [New = {EventName, NewEnable} | T1],
    [Old = {EventName, OldEnable} | T2],
    Added,
    Removed
) ->
    case {NewEnable, OldEnable} of
        {true, false} -> diff_hooks(T1, T2, [New | Added], Removed);
        {false, true} -> diff_hooks(T1, T2, Added, [Old | Removed])
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    ok = emqx_config_handler:add_handler(?CONF_KEY_PATH, ?MODULE),
    State = #state{sysdescr = iolist_to_binary(sysdescr())},
    load_event_hooks(sys_event_messages()),
    {ok, heartbeat(tick(State))}.

heartbeat(State) ->
    State#state{heartbeat = start_timer(sys_heatbeat_interval(), heartbeat)}.
tick(State) ->
    State#state{ticker = start_timer(sys_interval(), tick)}.

load_event_hooks([]) ->
    ok;
load_event_hooks(Events) ->
    lists:foreach(
        fun
            ({_, false}) ->
                ok;
            ({K, true}) ->
                {HookPoint, Fun} = hook_and_fun(K),
                emqx_hooks:put(HookPoint, {?MODULE, Fun, []}, ?HP_SYS_MSGS)
        end,
        Events
    ).

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info({timeout, TRef, heartbeat}, State = #state{heartbeat = TRef}) ->
    publish_any(uptime, integer_to_binary(uptime())),
    publish_any(datetime, iolist_to_binary(datetime())),
    {noreply, heartbeat(State)};
handle_info({timeout, TRef, tick}, State = #state{ticker = TRef, sysdescr = Descr}) ->
    publish_any(version, version()),
    publish_any(sysdescr, Descr),
    publish_any(brokers, mria:running_nodes()),
    publish_any(stats, emqx_stats:getstats()),
    publish_any(metrics, emqx_metrics:all()),
    {noreply, tick(State), hibernate};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, #state{heartbeat = TRef1, ticker = TRef2}) ->
    _ = emqx_config_handler:remove_handler(?CONF_KEY_PATH),
    unload_event_hooks(sys_event_messages()),
    lists:foreach(fun emqx_utils:cancel_timer/1, [TRef1, TRef2]).

unload_event_hooks([]) ->
    ok;
unload_event_hooks(Event) ->
    lists:foreach(
        fun({K, _}) ->
            {HookPoint, Fun} = hook_and_fun(K),
            emqx_hooks:del(HookPoint, {?MODULE, Fun})
        end,
        Event
    ).

%%--------------------------------------------------------------------
%% hook callbacks
%%--------------------------------------------------------------------

on_client_connected(ClientInfo, ConnInfo) ->
    Payload0 = common_infos(ClientInfo, ConnInfo),
    Payload = Payload0#{
        keepalive => maps:get(keepalive, ConnInfo, 0),
        clean_start => maps:get(clean_start, ConnInfo, true),
        expiry_interval => maps:get(expiry_interval, ConnInfo, 0)
    },
    publish(connected, Payload).

on_client_disconnected(
    ClientInfo,
    Reason,
    ConnInfo = #{disconnected_at := DisconnectedAt}
) ->
    Payload0 = common_infos(ClientInfo, ConnInfo),
    Payload = Payload0#{
        reason => reason(Reason),
        disconnected_at => DisconnectedAt
    },
    publish(disconnected, Payload).

-compile({inline, [reason/1]}).
reason(Reason) when is_atom(Reason) -> Reason;
reason({shutdown, Reason}) when is_atom(Reason) -> Reason;
reason({Error, _}) when is_atom(Error) -> Error;
reason(_) -> internal_error.

on_client_subscribed(
    _ClientInfo = #{
        clientid := ClientId,
        username := Username,
        protocol := Protocol
    },
    Topic,
    SubOpts
) ->
    Payload = #{
        clientid => ClientId,
        username => Username,
        protocol => Protocol,
        topic => emqx_topic:maybe_format_share(Topic),
        subopts => SubOpts,
        ts => erlang:system_time(millisecond)
    },
    publish(subscribed, Payload).

on_client_unsubscribed(
    _ClientInfo = #{
        clientid := ClientId,
        username := Username,
        protocol := Protocol
    },
    Topic,
    _SubOpts
) ->
    Payload = #{
        clientid => ClientId,
        username => Username,
        protocol => Protocol,
        topic => emqx_topic:maybe_format_share(Topic),
        ts => erlang:system_time(millisecond)
    },
    publish(unsubscribed, Payload).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

hook_and_fun(client_connected) ->
    {'client.connected', on_client_connected};
hook_and_fun(client_disconnected) ->
    {'client.disconnected', on_client_disconnected};
hook_and_fun(client_subscribed) ->
    {'session.subscribed', on_client_subscribed};
hook_and_fun(client_unsubscribed) ->
    {'session.unsubscribed', on_client_unsubscribed}.

publish_any(Name, Value) ->
    _ = publish(Name, Value),
    ok.

publish(uptime, Uptime) ->
    safe_publish(systop(uptime), Uptime);
publish(datetime, Datetime) ->
    safe_publish(systop(datetime), Datetime);
publish(version, Version) ->
    safe_publish(systop(version), #{retain => true}, Version);
publish(sysdescr, Descr) ->
    safe_publish(systop(sysdescr), #{retain => true}, Descr);
publish(brokers, Nodes) ->
    Payload = string:join([atom_to_list(N) || N <- Nodes], ","),
    safe_publish(<<"$SYS/brokers">>, #{retain => true}, Payload);
publish(stats, Stats) ->
    [
        safe_publish(systop(stats_topic(Stat)), integer_to_binary(Val))
     || {Stat, Val} <- Stats, is_atom(Stat), is_integer(Val)
    ];
publish(metrics, Metrics) ->
    [
        safe_publish(systop(metric_topic(Name)), integer_to_binary(Val))
     || {Name, Val} <- Metrics, is_atom(Name), is_integer(Val)
    ];
publish(Event, Payload) when
    Event == connected;
    Event == disconnected;
    Event == subscribed;
    Event == unsubscribed
->
    Topic = event_topic(Event, Payload),
    safe_publish(Topic, emqx_utils_json:encode(Payload)).

metric_topic(Name) ->
    translate_topic("metrics/", Name).

stats_topic(Name) ->
    translate_topic("stats/", Name).

translate_topic(Prefix, Name) ->
    lists:concat([Prefix, string:replace(atom_to_list(Name), ".", "/", all)]).

safe_publish(Topic, Payload) ->
    safe_publish(Topic, #{}, Payload).
safe_publish(Topic, Flags, Payload) ->
    emqx_broker:safe_publish(
        emqx_message:set_flags(
            maps:merge(#{sys => true}, Flags),
            emqx_message:make(?SYS, Topic, iolist_to_binary(Payload))
        )
    ).

common_infos(
    _ClientInfo = #{
        clientid := ClientId,
        username := Username,
        peerhost := PeerHost,
        sockport := SockPort,
        protocol := Protocol
    },
    _ConnInfo = #{
        proto_name := ProtoName,
        proto_ver := ProtoVer,
        connected_at := ConnectedAt
    }
) ->
    #{
        clientid => ClientId,
        username => Username,
        ipaddress => ntoa(PeerHost),
        sockport => SockPort,
        protocol => Protocol,
        proto_name => ProtoName,
        proto_ver => ProtoVer,
        connected_at => ConnectedAt,
        ts => erlang:system_time(millisecond)
    }.

ntoa(undefined) -> undefined;
ntoa({IpAddr, Port}) -> iolist_to_binary([inet:ntoa(IpAddr), ":", integer_to_list(Port)]);
ntoa(IpAddr) -> iolist_to_binary(inet:ntoa(IpAddr)).

event_topic(Event, #{clientid := ClientId, protocol := mqtt}) ->
    iolist_to_binary(
        [systop("clients"), "/", ClientId, "/", atom_to_binary(Event)]
    );
event_topic(Event, #{clientid := ClientId, protocol := GwName}) ->
    iolist_to_binary(
        [
            systop("gateway"),
            "/",
            bin(GwName),
            "/clients/",
            ClientId,
            "/",
            bin(Event)
        ]
    ).

bin(A) when is_atom(A) -> atom_to_binary(A);
bin(B) when is_binary(B) -> B.
