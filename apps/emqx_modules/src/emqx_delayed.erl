%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_delayed).

-behaviour(gen_server).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).

-export([
    start_link/0,
    on_message_publish/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% gen_server callbacks
-export([
    load/0,
    unload/0,
    load_or_unload/1,
    get_conf/1,
    update_config/1,
    list/1,
    get_delayed_message/1,
    get_delayed_message/2,
    delete_delayed_message/1,
    delete_delayed_message/2,
    cluster_list/1
]).

%% exports for query
-export([
    qs2ms/2,
    format_delayed/1,
    format_delayed/2
]).

-export([
    post_config_update/5
]).

%% exported for `emqx_telemetry'
-export([get_basic_usage_info/0]).

-record(delayed_message, {key, delayed, msg}).
-type delayed_message() :: #delayed_message{}.
-type with_id_return() :: ok | {error, not_found}.
-type with_id_return(T) :: {ok, T} | {error, not_found}.

-export_type([with_id_return/0, with_id_return/1]).

-type state() :: #{
    publish_timer := maybe(timer:tref()),
    publish_at := non_neg_integer(),
    stats_timer := maybe(reference()),
    stats_fun := maybe(fun((pos_integer()) -> ok))
}.

%% sync ms with record change
-define(QUERY_MS(Id), [{{delayed_message, {'_', Id}, '_', '_'}, [], ['$_']}]).
-define(DELETE_MS(Id), [{{delayed_message, {'$1', Id}, '_', '_'}, [], ['$1']}]).

-define(TAB, ?MODULE).
-define(SERVER, ?MODULE).
-define(MAX_INTERVAL, 4294967).
-define(FORMAT_FUN, {?MODULE, format_delayed}).
-define(NOW, erlang:system_time(milli_seconds)).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------
mnesia(boot) ->
    ok = mria:create_table(?TAB, [
        {type, ordered_set},
        {storage, disc_copies},
        {local_content, true},
        {record_name, delayed_message},
        {attributes, record_info(fields, delayed_message)}
    ]).

%%--------------------------------------------------------------------
%% Hooks
%%--------------------------------------------------------------------
on_message_publish(
    Msg = #message{
        id = Id,
        topic = <<"$delayed/", Topic/binary>>,
        timestamp = Ts
    }
) ->
    [Delay, Topic1] = binary:split(Topic, <<"/">>),
    {PubAt, Delayed} =
        case binary_to_integer(Delay) of
            Interval when Interval < ?MAX_INTERVAL ->
                {Interval * 1000 + Ts, Interval};
            Timestamp ->
                %% Check malicious timestamp?
                Internal = Timestamp - erlang:round(Ts / 1000),
                case Internal > ?MAX_INTERVAL of
                    true -> error(invalid_delayed_timestamp);
                    false -> {Timestamp * 1000, Internal}
                end
        end,
    PubMsg = Msg#message{topic = Topic1},
    Headers = PubMsg#message.headers,
    case store(#delayed_message{key = {PubAt, Id}, delayed = Delayed, msg = PubMsg}) of
        ok -> ok;
        {error, Error} -> ?SLOG(error, #{msg => "store_delayed_message_fail", error => Error})
    end,
    {stop, PubMsg#message{headers = Headers#{allow_publish => false}}};
on_message_publish(Msg) ->
    {ok, Msg}.

%%--------------------------------------------------------------------
%% Start delayed publish server
%%--------------------------------------------------------------------

-spec start_link() -> emqx_types:startlink_ret().
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec store(delayed_message()) -> ok | {error, atom()}.
store(DelayedMsg) ->
    gen_server:call(?SERVER, {store, DelayedMsg}, infinity).

get_conf(Key) ->
    emqx_conf:get([delayed, Key]).

load() ->
    load_or_unload(true).

unload() ->
    load_or_unload(false).

load_or_unload(Bool) ->
    gen_server:call(?SERVER, {do_load_or_unload, Bool}).

list(Params) ->
    emqx_mgmt_api:paginate(?TAB, Params, ?FORMAT_FUN).

cluster_list(Params) ->
    emqx_mgmt_api:cluster_query(
        ?TAB,
        Params,
        [],
        fun ?MODULE:qs2ms/2,
        fun ?MODULE:format_delayed/2
    ).

-spec qs2ms(atom(), {list(), list()}) -> emqx_mgmt_api:match_spec_and_filter().
qs2ms(_Table, {_Qs, _Fuzzy}) ->
    #{
        match_spec => [{'$1', [], ['$1']}],
        fuzzy_fun => undefined
    }.

format_delayed(Delayed) ->
    format_delayed(node(), Delayed).

format_delayed(WhichNode, Delayed) ->
    format_delayed(WhichNode, Delayed, false).

format_delayed(
    WhichNode,
    #delayed_message{
        key = {ExpectTimeStamp, Id},
        delayed = Delayed,
        msg = #message{
            topic = Topic,
            from = From,
            headers = Headers,
            qos = Qos,
            timestamp = PublishTimeStamp,
            payload = Payload
        }
    },
    WithPayload
) ->
    PublishTime = to_rfc3339(PublishTimeStamp div 1000),
    ExpectTime = to_rfc3339(ExpectTimeStamp div 1000),
    RemainingTime = ExpectTimeStamp - ?NOW,
    Result = #{
        msgid => emqx_guid:to_hexstr(Id),
        node => WhichNode,
        publish_at => PublishTime,
        delayed_interval => Delayed,
        delayed_remaining => RemainingTime div 1000,
        expected_at => ExpectTime,
        topic => Topic,
        qos => Qos,
        from_clientid => From,
        from_username => maps:get(username, Headers, undefined)
    },
    case WithPayload of
        true ->
            Result#{payload => Payload};
        _ ->
            Result
    end.

to_rfc3339(Timestamp) ->
    list_to_binary(calendar:system_time_to_rfc3339(Timestamp, [{unit, second}])).

-spec get_delayed_message(binary()) -> with_id_return(map()).
get_delayed_message(Id) ->
    case ets:select(?TAB, ?QUERY_MS(Id)) of
        [] ->
            {error, not_found};
        Rows ->
            Message = hd(Rows),
            {ok, format_delayed(node(), Message, true)}
    end.

get_delayed_message(Node, Id) when Node =:= node() ->
    get_delayed_message(Id);
get_delayed_message(Node, Id) ->
    emqx_delayed_proto_v1:get_delayed_message(Node, Id).

-spec delete_delayed_message(binary()) -> with_id_return().
delete_delayed_message(Id) ->
    case ets:select(?TAB, ?DELETE_MS(Id)) of
        [] ->
            {error, not_found};
        Rows ->
            Timestamp = hd(Rows),
            mria:dirty_delete(?TAB, {Timestamp, Id})
    end.

delete_delayed_message(Node, Id) when Node =:= node() ->
    delete_delayed_message(Id);
delete_delayed_message(Node, Id) ->
    emqx_delayed_proto_v1:delete_delayed_message(Node, Id).

update_config(Config) ->
    emqx_conf:update([delayed], Config, #{rawconf_with_defaults => true, override_to => cluster}).

post_config_update(_KeyPath, _ConfigReq, NewConf, _OldConf, _AppEnvs) ->
    Enable = maps:get(enable, NewConf, undefined),
    load_or_unload(Enable).

%%--------------------------------------------------------------------
%% gen_server callback
%%--------------------------------------------------------------------

init([]) ->
    ok = mria:wait_for_tables([?TAB]),
    erlang:process_flag(trap_exit, true),
    emqx_conf:add_handler([delayed], ?MODULE),
    State =
        ensure_stats_event(
            ensure_publish_timer(#{
                publish_timer => undefined,
                publish_at => 0,
                stats_timer => undefined,
                stats_fun => undefined
            })
        ),
    {ok, do_load_or_unload(emqx:get_config([delayed, enable]), State)}.

handle_call({store, DelayedMsg = #delayed_message{key = Key}}, _From, State) ->
    Size = mnesia:table_info(?TAB, size),
    case get_conf(max_delayed_messages) of
        0 ->
            ok = mria:dirty_write(?TAB, DelayedMsg),
            emqx_metrics:inc('messages.delayed'),
            {reply, ok, ensure_publish_timer(Key, State)};
        Max when Size >= Max ->
            {reply, {error, max_delayed_messages_full}, State};
        Max when Size < Max ->
            ok = mria:dirty_write(?TAB, DelayedMsg),
            emqx_metrics:inc('messages.delayed'),
            {reply, ok, ensure_publish_timer(Key, State)}
    end;
handle_call({do_load_or_unload, Bool}, _From, State0) ->
    State = do_load_or_unload(Bool, State0),
    {reply, ok, State};
handle_call(Req, _From, State) ->
    ?tp(error, emqx_delayed_unexpected_call, #{call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?tp(error, emqx_delayed_unexpected_cast, #{cast => Msg}),
    {noreply, State}.

%% Do Publish...
handle_info({timeout, TRef, do_publish}, State = #{publish_timer := TRef}) ->
    DeletedKeys = do_publish(mnesia:dirty_first(?TAB), ?NOW),
    lists:foreach(fun(Key) -> mria:dirty_delete(?TAB, Key) end, DeletedKeys),
    {noreply, ensure_publish_timer(State#{publish_timer := undefined, publish_at := 0})};
handle_info(stats, State = #{stats_fun := StatsFun}) ->
    StatsTimer = erlang:send_after(timer:seconds(1), self(), stats),
    StatsFun(delayed_count()),
    {noreply, State#{stats_timer := StatsTimer}, hibernate};
handle_info(Info, State) ->
    ?tp(error, emqx_delayed_unexpected_info, #{info => Info}),
    {noreply, State}.

terminate(_Reason, #{stats_timer := StatsTimer} = State) ->
    emqx_conf:remove_handler([delayed]),
    emqx_misc:cancel_timer(StatsTimer),
    do_load_or_unload(false, State).

code_change(_Vsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Telemetry
%%--------------------------------------------------------------------

-spec get_basic_usage_info() -> #{delayed_message_count => non_neg_integer()}.
get_basic_usage_info() ->
    DelayedCount =
        case ets:info(?TAB, size) of
            undefined -> 0;
            Num -> Num
        end,
    #{delayed_message_count => DelayedCount}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% Ensure the stats
-spec ensure_stats_event(state()) -> state().
ensure_stats_event(State) ->
    StatsFun = emqx_stats:statsfun('delayed.count', 'delayed.max'),
    StatsTimer = erlang:send_after(timer:seconds(1), self(), stats),
    State#{stats_fun := StatsFun, stats_timer := StatsTimer}.

%% Ensure publish timer
-spec ensure_publish_timer(state()) -> state().
ensure_publish_timer(State) ->
    ensure_publish_timer(mnesia:dirty_first(?TAB), State).

ensure_publish_timer('$end_of_table', State) ->
    State#{publish_timer := undefined, publish_at := 0};
ensure_publish_timer({Ts, _Id}, State = #{publish_timer := undefined}) ->
    ensure_publish_timer(Ts, ?NOW, State);
ensure_publish_timer({Ts, _Id}, State = #{publish_timer := TRef, publish_at := PubAt}) when
    Ts < PubAt
->
    ok = emqx_misc:cancel_timer(TRef),
    ensure_publish_timer(Ts, ?NOW, State);
ensure_publish_timer(_Key, State) ->
    State.

ensure_publish_timer(Ts, Now, State) ->
    Interval = max(1, Ts - Now),
    TRef = emqx_misc:start_timer(Interval, do_publish),
    State#{publish_timer := TRef, publish_at := Now + Interval}.

do_publish(Key, Now) ->
    do_publish(Key, Now, []).

%% Do publish
do_publish('$end_of_table', _Now, Acc) ->
    Acc;
do_publish({Ts, _Id}, Now, Acc) when Ts > Now ->
    Acc;
do_publish(Key = {Ts, _Id}, Now, Acc) when Ts =< Now ->
    case mnesia:dirty_read(?TAB, Key) of
        [] ->
            ok;
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
    do_publish(mnesia:dirty_next(?TAB, Key), Now, [Key | Acc]).

-spec delayed_count() -> non_neg_integer().
delayed_count() -> mnesia:table_info(?TAB, size).

do_load_or_unload(true, State) ->
    emqx_hooks:put('message.publish', {?MODULE, on_message_publish, []}, ?HP_DELAY_PUB),
    State;
do_load_or_unload(false, #{publish_timer := PubTimer} = State) ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    emqx_misc:cancel_timer(PubTimer),
    ets:delete_all_objects(?TAB),
    State#{publish_timer := undefined, publish_at := 0};
do_load_or_unload(_, State) ->
    State.
