%%--------------------------------------------------------------------
%% Copyright (c) 2018-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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


-export([ start_link/0
        , stop/0
        ]).

-export([ version/0
        , uptime/0
        , datetime/0
        , sysdescr/0
        ]).

-export([info/0]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        ]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-import(emqx_topic, [systop/1]).
-import(emqx_misc, [start_timer/2]).

-record(state,
        {heartbeat  :: maybe(reference())
        , ticker     :: maybe(reference())
        , sysdescr   :: binary()
        }).

-define(APP, emqx).
-define(SYS, ?MODULE).

-define(INFO_KEYS,
        [ version  % Broker version
        , uptime   % Broker uptime
        , datetime % Broker local datetime
        , sysdescr % Broker description
        ]).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

-spec(start_link() -> {ok, pid()} | ignore | {error, any()}).
start_link() ->
    gen_server:start_link({local, ?SYS}, ?MODULE, [], []).

stop() ->
    gen_server:stop(?SYS).

%% @doc Get sys version
-spec(version() -> string()).
version() -> emqx_app:get_release().

%% @doc Get sys description
-spec(sysdescr() -> string()).
sysdescr() -> emqx_app:get_description().

%% @doc Get sys uptime
-spec(uptime() -> Milliseconds :: integer()).
uptime() ->
    {TotalWallClock, _} = erlang:statistics(wall_clock),
    TotalWallClock.

%% @doc Get sys datetime
-spec(datetime() -> string()).
datetime() ->
    {{Y, M, D}, {H, MM, S}} = calendar:local_time(),
    lists:flatten(
        io_lib:format(
            "~4..0w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w", [Y, M, D, H, MM, S])).

sys_interval() ->
    emqx:get_config([broker, sys_msg_interval]).

sys_heatbeat_interval() ->
    emqx:get_config([broker, sys_heartbeat_interval]).

%% @doc Get sys info
-spec(info() -> list(tuple())).
info() ->
    [{version, version()},
     {sysdescr, sysdescr()},
     {uptime, uptime()},
     {datetime, datetime()}].

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([]) ->
    State = #state{sysdescr   = iolist_to_binary(sysdescr())},
    {ok, heartbeat(tick(State))}.

heartbeat(State) ->
    State#state{heartbeat = start_timer(sys_heatbeat_interval(), heartbeat)}.
tick(State) ->
    State#state{ticker = start_timer(sys_interval(), tick)}.

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
    publish_any(brokers, mria_mnesia:running_nodes()),
    publish_any(stats, emqx_stats:getstats()),
    publish_any(metrics, emqx_metrics:all()),
    {noreply, tick(State), hibernate};

handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, #state{heartbeat = TRef1, ticker = TRef2}) ->
    lists:foreach(fun emqx_misc:cancel_timer/1, [TRef1, TRef2]).

%%-----------------------------------------------------------------------------
%% Internal functions
%%-----------------------------------------------------------------------------

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
    [safe_publish(systop(lists:concat(['stats/', Stat])), integer_to_binary(Val))
     || {Stat, Val} <- Stats, is_atom(Stat), is_integer(Val)];
publish(metrics, Metrics) ->
    [safe_publish(systop(metric_topic(Name)), integer_to_binary(Val))
     || {Name, Val} <- Metrics, is_atom(Name), is_integer(Val)].

metric_topic(Name) ->
    lists:concat(["metrics/", string:replace(atom_to_list(Name), ".", "/", all)]).

safe_publish(Topic, Payload) ->
    safe_publish(Topic, #{}, Payload).
safe_publish(Topic, Flags, Payload) ->
    emqx_broker:safe_publish(
      emqx_message:set_flags(
        maps:merge(#{sys => true}, Flags),
        emqx_message:make(?SYS, Topic, iolist_to_binary(Payload)))).
