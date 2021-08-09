%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_collection).

-behaviour(gen_server).

-include("emqx_dashboard.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-export([ start_link/0
        ]).

-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-export([get_collect/0]).

-export([get_local_time/0]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% Mnesia bootstrap
-export([mnesia/1]).

-define(APP, emqx_dashboard).

-define(DEFAULT_INTERVAL, 10). %% seconds

-define(COLLECT, {[],[],[]}).

-define(CLEAR_INTERVAL, 86400000).

-define(EXPIRE_INTERVAL, 86400000 * 7).

mnesia(boot) ->
    ok = ekka_mnesia:create_table(emqx_collect, [
        {type, set},
        {local_content, true},
        {disc_only_copies, [node()]},
        {record_name, mqtt_collect},
        {attributes, record_info(fields, mqtt_collect)}]);
mnesia(copy) ->
    mnesia:add_table_copy(emqx_collect, node(), disc_only_copies).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_collect() -> gen_server:call(whereis(?MODULE), get_collect).

init([]) ->
    timer(timer:seconds(interval()), collect),
    timer(get_today_remaining_seconds(), clear_expire_data),
    ExpireInterval = emqx_config:get([emqx_dashboard, monitor, interval], ?EXPIRE_INTERVAL),
    State = #{
        count => count(),
        expire_interval => ExpireInterval,
        collect => ?COLLECT,
        temp_collect => {0, 0, 0, 0},
        last_collects => {0, 0, 0}
    },
    {ok, State}.

interval() ->
    emqx_config:get([?APP, sample_interval], ?DEFAULT_INTERVAL).

count() ->
    60 div interval().

handle_call(get_collect, _From, State = #{temp_collect := {Received, Sent, _, _}}) ->
    {reply, {Received, Sent, collect(subscriptions), collect(connections)}, State, hibernate};
handle_call(_Req, _From, State) ->
    {reply, ok, State}.
handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(collect, State = #{collect := Collect, count := 1, temp_collect := TempCollect, last_collects := LastCollect}) ->
    NewLastCollect = flush(collect_all(Collect), LastCollect),
    TempCollect1 = temp_collect(TempCollect),
    timer(timer:seconds(interval()), collect),
    {noreply, State#{count => count(),
                     collect => ?COLLECT,
                     temp_collect => TempCollect1,
                     last_collects => NewLastCollect}};

handle_info(collect, State = #{count := Count, collect := Collect, temp_collect := TempCollect}) ->
    TempCollect1 = temp_collect(TempCollect),
    timer(timer:seconds(interval()), collect),
    {noreply, State#{count => Count - 1,
                     collect => collect_all(Collect),
                     temp_collect => TempCollect1}, hibernate};

handle_info(clear_expire_data, State = #{expire_interval := ExpireInterval}) ->
    timer(?CLEAR_INTERVAL, clear_expire_data),
    T1 = get_local_time(),
    Spec = ets:fun2ms(fun({_, T, _C} = Data) when (T1 - T) > ExpireInterval -> Data end),
    Collects = dets:select(emqx_collect, Spec),
    lists:foreach(fun(Collect) ->
        dets:delete_object(emqx_collect, Collect)
    end, Collects),
    {noreply, State, hibernate};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

temp_collect({_, _, Received, Sent}) ->
    Received1 = collect(received),
    Sent1 = collect(sent),
    {(Received1 - Received) div interval(),
     (Sent1 - Sent) div interval(),
     Received1,
     Sent1}.

collect_all({Connection, Route, Subscription}) ->
    {[collect(connections)| Connection],
     [collect(routes)| Route],
     [collect(subscriptions)| Subscription]}.

collect(connections) ->
    emqx_stats:getstat('connections.count');
collect(routes) ->
    emqx_stats:getstat('routes.count');
collect(subscriptions) ->
    emqx_stats:getstat('subscriptions.count');
collect(received) ->
    emqx_metrics:val('messages.received');
collect(sent) ->
    emqx_metrics:val('messages.sent');
collect(dropped) ->
    emqx_metrics:val('messages.dropped').

flush({Connection, Route, Subscription}, {Received0, Sent0, Dropped0}) ->
    Received = collect(received),
    Sent = collect(sent),
    Dropped = collect(dropped),
    Collect = {avg(Connection),
               avg(Route),
               avg(Subscription),
               diff(Received, Received0),
               diff(Sent, Sent0),
               diff(Dropped, Dropped0)},
    Ts = get_local_time(),
    _ = mnesia:dirty_write(emqx_collect, #mqtt_collect{timestamp = Ts, collect = Collect}),
    {Received, Sent, Dropped}.

avg(Items) ->
    lists:sum(Items) div count().

diff(Item0, Item1) ->
    Item0 - Item1.

timer(Secs, Msg) ->
    erlang:send_after(Secs, self(), Msg).

get_today_remaining_seconds() ->
    ?CLEAR_INTERVAL - (get_local_time() rem ?CLEAR_INTERVAL).

get_local_time() ->
    (calendar:datetime_to_gregorian_seconds(calendar:local_time()) -
        calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}})) * 1000.
