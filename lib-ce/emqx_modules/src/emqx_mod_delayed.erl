%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mod_delayed).

-behaviour(gen_server).
-behaviour(emqx_gen_mod).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-logger_header("[Delayed]").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% emqx_gen_mod callbacks
-export([ load/1
        , unload/1
        , description/0
        ]).

-export([ start_link/0
        , on_message_publish/1
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(delayed_message,
        { key
        , msg
        }).
-type delayed_message() :: #delayed_message{}.

-define(TAB, ?MODULE).
-define(SERVER, ?MODULE).
-define(MAX_INTERVAL, 4294967).

-type state() :: #{ publish_at := non_neg_integer()
                  , timer := timer:tref() | undefined
                  , stats_timer => timer:tref() | undefined
                  , stats_fun => function()
                  }.

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = ekka_mnesia:create_table(?TAB, [
                {type, ordered_set},
                {disc_copies, [node()]},
                {local_content, true},
                {record_name, delayed_message},
                {attributes, record_info(fields, delayed_message)}]);
mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?TAB, disc_copies).

%%--------------------------------------------------------------------
%% Load/Unload
%%--------------------------------------------------------------------

-spec(load(list()) -> ok).
load(_Env) ->
    emqx_mod_sup:start_child(?MODULE, worker),
    emqx:hook('message.publish', {?MODULE, on_message_publish, []}).

-spec(unload(list()) -> ok).
unload(_Env) ->
    emqx:unhook('message.publish', {?MODULE, on_message_publish}),
    emqx_mod_sup:stop_child(?MODULE).

description() ->
    "EMQX Delayed Publish Module".
%%--------------------------------------------------------------------
%% Hooks
%%--------------------------------------------------------------------

on_message_publish(Msg = #message{
                            id = Id,
                            topic = <<"$delayed/", Topic/binary>>,
                            timestamp = Ts
                           }) ->
    [Delay, Topic1] = binary:split(Topic, <<"/">>),
    PubAt = case binary_to_integer(Delay) of
                Interval when Interval < ?MAX_INTERVAL ->
                    Interval + erlang:round(Ts / 1000);
                Timestamp ->
                    %% Check malicious timestamp?
                    case (Timestamp - erlang:round(Ts / 1000)) > ?MAX_INTERVAL of
                        true  -> error(invalid_delayed_timestamp);
                        false -> Timestamp
                    end
            end,
    PubMsg = Msg#message{topic = Topic1},
    Headers = PubMsg#message.headers,
    ok = store(#delayed_message{key = {PubAt, Id}, msg = PubMsg}),
    {stop, PubMsg#message{headers = Headers#{allow_publish => false}}};

on_message_publish(Msg) ->
    {ok, Msg}.

%%--------------------------------------------------------------------
%% Start delayed publish server
%%--------------------------------------------------------------------

-spec(start_link() -> emqx_types:startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec(store(delayed_message()) -> ok).
store(DelayedMsg) ->
    gen_server:call(?SERVER, {store, DelayedMsg}, infinity).

%%--------------------------------------------------------------------
%% gen_server callback
%%--------------------------------------------------------------------

init([]) ->
    {ok, ensure_stats_event(
           ensure_publish_timer(#{timer => undefined,
                                  publish_at => 0,
                                  stats_timer => undefined}))}.

handle_call({store, DelayedMsg = #delayed_message{key = Key}}, _From, State) ->
    ok = mnesia:dirty_write(?TAB, DelayedMsg),
    emqx_metrics:inc('messages.delayed'),
    {reply, ok, ensure_publish_timer(Key, State)};

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

%% Do Publish...
handle_info({timeout, TRef, do_publish}, State = #{timer := TRef}) ->
    DeletedKeys = do_publish(mnesia:dirty_first(?TAB), erlang:system_time(seconds)),
    lists:foreach(fun(Key) -> mnesia:dirty_delete(?TAB, Key) end, DeletedKeys),
    {noreply, ensure_publish_timer(State#{timer := undefined, publish_at := 0})};

handle_info(stats, State = #{stats_fun := StatsFun}) ->
    StatsFun(delayed_count()),
    {noreply, State, hibernate};

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #{timer := PublishTimer} = State) ->
    emqx_misc:cancel_timer(PublishTimer),
    emqx_misc:cancel_timer(maps:get(stats_timer, State, undefined)).

code_change({down, Vsn}, State, _Extra) when Vsn =:= "4.3.0" ->
    NState = maps:with([timer, publish_at], State),
    {ok, NState};

code_change(Vsn, State, _Extra) when Vsn =:= "4.3.0" ->
    NState = ensure_stats_event(State),
    {ok, NState}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% Ensure the stats
-spec ensure_stats_event(state()) -> state().
ensure_stats_event(State) ->
    StatsFun = emqx_stats:statsfun('delayed.count', 'delayed.max'),
    {ok, StatsTimer} = timer:send_interval(timer:seconds(1), stats),
    State#{stats_fun => StatsFun, stats_timer => StatsTimer}.

%% Ensure publish timer
-spec ensure_publish_timer(state()) -> state().
ensure_publish_timer(State) ->
    ensure_publish_timer(mnesia:dirty_first(?TAB), State).

ensure_publish_timer('$end_of_table', State) ->
    State#{timer := undefined, publish_at := 0};
ensure_publish_timer({Ts, _Id}, State = #{timer := undefined}) ->
    ensure_publish_timer(Ts, erlang:system_time(seconds), State);
ensure_publish_timer({Ts, _Id}, State = #{timer := TRef, publish_at := PubAt})
    when Ts < PubAt ->
    ok = emqx_misc:cancel_timer(TRef),
    ensure_publish_timer(Ts, erlang:system_time(seconds), State);
ensure_publish_timer(_Key, State) ->
    State.

ensure_publish_timer(Ts, Now, State) ->
    Interval = max(1, Ts - Now),
    TRef = emqx_misc:start_timer(timer:seconds(Interval), do_publish),
    State#{timer := TRef, publish_at := Now + Interval}.

do_publish(Key, Now) ->
    do_publish(Key, Now, []).

%% Do publish
do_publish('$end_of_table', _Now, Acc) ->
    Acc;
do_publish({Ts, _Id}, Now, Acc) when Ts > Now ->
    Acc;
do_publish(Key = {Ts, _Id}, Now, Acc) when Ts =< Now ->
    case mnesia:dirty_read(?TAB, Key) of
        [] -> ok;
        [#delayed_message{msg = Msg}] ->
            case emqx_banned:look_up({clientid, Msg#message.from}) of
                [] ->
                    emqx_pool:async_submit(fun emqx:publish/1, [Msg]);
                _ ->
                    ?tp(
                        notice,
                        ignore_delayed_message_publish,
                        #{
                            reason => "client is banned",
                            clienid => Msg#message.from
                        }
                    ),
                    ok
            end
    end,
    do_publish(mnesia:dirty_next(?TAB, Key), Now, [Key|Acc]).

-spec(delayed_count() -> non_neg_integer()).
delayed_count() -> mnesia:table_info(?TAB, size).
