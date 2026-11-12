%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_cleanup_worker).
-behaviour(gen_server).

-moduledoc """
Release quota of consumed and expired records of reject queues.

The workers form a pool. The queue id selects the worker, so one worker handles all reports of a queue.
Consumers report the boundary below which every record of a shard is consumed.
Garbage collection reports the boundary below which every record of a shard is expired.
The worker calls `emqx_mq_message_db:release_before/3` for the reported boundary.
That function deletes the records before it releases quota.

The worker waits two quota flush intervals before it processes a new boundary.
Records below the boundary were stored before they were consumed, so the index counts them by then.
After a release, no more quota below the boundary can be released until new records arrive.

The worker monitors each reporting consumer and keeps the released boundary of each of its shards.
It ignores reports at or below that boundary, so repeated reports of an idle queue do not read the index.
When the consumer stops, the worker drops its boundaries.
Consumers repeat their latest boundary periodically, so a restarted worker recovers it.
Expiry reports have no consumer. The worker processes each of them once.

The pool is separate from the deletion pool, so quota release does not wait for queue deletion or purge.
When cleanup falls behind, the queue stays full and rejects new records, so the pending work stays bounded.
""".

-include("emqx_mq_internal.hrl").

-export([start_link/2, report_consumed/3, report_expired/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% For testing
-export([retry_interval/0, process_delay/0]).

-record(report, {
    mq :: emqx_mq_types:mq(),
    shard :: emqx_ds:shard(),
    %% Exclusive DS timestamp boundary. Every record of the shard below it is consumed or expired.
    boundary :: emqx_ds:time(),
    %% The reporting consumer, or `expired` for a report from garbage collection
    owner :: pid() | expired
}).

-define(RETRY_INTERVAL, 1000).
%% Retries of a failing release double the delay up to this many retry intervals.
-define(MAX_RETRY_FACTOR, 16).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link(atom(), pos_integer()) -> gen_server:start_ret().
start_link(Pool, Index) ->
    gen_server:start_link(?MODULE, {Pool, Index}, []).

-doc "Report from the consumer that every record of the shard below the boundary is consumed.".
-spec report_consumed(emqx_mq_types:mq(), emqx_ds:shard(), emqx_ds:time()) ->
    ok | {error, no_cleanup_worker}.
report_consumed(MQ, Shard, Boundary) ->
    report(MQ, Shard, Boundary, self()).

-doc "Report that every record of the shard below the boundary is expired.".
-spec report_expired(emqx_mq_types:mq(), emqx_ds:shard(), emqx_ds:time()) ->
    ok | {error, no_cleanup_worker}.
report_expired(MQ, Shard, Boundary) ->
    report(MQ, Shard, Boundary, expired).

-spec retry_interval() -> pos_integer().
retry_interval() ->
    ?RETRY_INTERVAL.

-spec process_delay() -> non_neg_integer().
process_delay() ->
    2 * emqx_mq_message_db:quota_buffer_flush_interval().

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init({Pool, Index}) ->
    process_flag(trap_exit, true),
    true = gproc_pool:connect_worker(Pool, {Pool, Index}),
    {ok, #{pool => Pool, index => Index, consumers => #{}, work => #{}}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(#report{} = Report, State) ->
    {noreply, handle_report(Report, State)};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({timeout, TRef, {work, Key}}, #{work := Work0} = State) ->
    case Work0 of
        #{Key := #{timer := TRef} = Item} ->
            Work =
                case work(Key, Item#{timer := undefined}) of
                    done -> maps:remove(Key, Work0);
                    {continue, Item1} -> Work0#{Key := Item1}
                end,
            {noreply, State#{work := Work}};
        #{} ->
            {noreply, State}
    end;
handle_info({'DOWN', _MRef, process, Pid, _Reason}, State) ->
    {noreply, handle_consumer_down(Pid, State)};
handle_info(_Message, State) ->
    ?tp_debug(mq_cleanup_unknown_message, #{message => _Message}),
    {noreply, State}.

terminate(_Reason, #{pool := Pool, index := Index}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Index}).

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

report(#{id := Id} = MQ, Shard, Boundary, Owner) ->
    ?tp_debug(mq_cleanup_report, #{id => Id, shard => Shard, boundary => Boundary, owner => Owner}),
    case pick_worker(Id) of
        {ok, Pid} ->
            gen_server:cast(Pid, #report{
                mq = MQ, shard = Shard, boundary = Boundary, owner = Owner
            });
        {error, _} = Error ->
            Error
    end.

%% The pool is missing when the services are stopped, and has no workers while they restart.
pick_worker(Id) ->
    try gproc_pool:pick_worker(?MODULE, Id) of
        false -> {error, no_cleanup_worker};
        Pid -> {ok, Pid}
    catch
        error:badarg -> {error, no_cleanup_worker}
    end.

handle_report(
    #report{mq = #{id := Id} = MQ, shard = Shard, boundary = Boundary, owner = Owner},
    #{work := Work0} = State0
) ->
    State = ensure_monitor(Owner, State0),
    Key = {Id, Shard, Owner},
    case Work0 of
        #{Key := #{boundary := Latest}} when Boundary =< Latest ->
            ?tp_debug(mq_cleanup_report_ignored, #{id => Id, shard => Shard, boundary => Boundary}),
            State;
        #{Key := Item0} ->
            Item = ensure_timer(Key, Item0#{mq := MQ, boundary := Boundary}),
            State#{work := Work0#{Key := Item}};
        #{} ->
            Item = ensure_timer(Key, #{
                mq => MQ, boundary => Boundary, failures => 0, timer => undefined
            }),
            State#{work := Work0#{Key => Item}}
    end.

ensure_monitor(expired, State) ->
    State;
ensure_monitor(Pid, #{consumers := Consumers} = State) ->
    case Consumers of
        #{Pid := _} -> State;
        #{} -> State#{consumers := Consumers#{Pid => erlang:monitor(process, Pid)}}
    end.

%% Drop the boundaries of a stopped consumer. Finish its pending work once, like an expiry report.
handle_consumer_down(Pid, #{consumers := Consumers, work := Work0} = State) ->
    Work = maps:fold(
        fun
            ({Id, Shard, Owner} = Key, Item, Acc0) when Owner =:= Pid ->
                Acc = maps:remove(Key, Acc0),
                case Item of
                    #{timer := undefined} ->
                        Acc;
                    #{mq := MQ, boundary := Boundary} ->
                        ok = cancel_timer(Item),
                        add_expired_work({Id, Shard, expired}, MQ, Boundary, Acc)
                end;
            (_Key, _Item, Acc) ->
                Acc
        end,
        Work0,
        Work0
    ),
    State#{consumers := maps:remove(Pid, Consumers), work := Work}.

add_expired_work(Key, MQ, Boundary, Work) ->
    case Work of
        #{Key := #{boundary := Latest} = Item} ->
            Work#{Key := ensure_timer(Key, Item#{boundary := max(Latest, Boundary)})};
        #{} ->
            Item = #{mq => MQ, boundary => Boundary, failures => 0, timer => undefined},
            Work#{Key => start_timer(Key, 0, Item)}
    end.

work({Id, Shard, Owner} = Key, #{mq := MQ, boundary := Boundary, failures := Failures} = Item) ->
    Result =
        try
            emqx_mq_message_db:release_before(MQ, Shard, Boundary)
        catch
            Class:Reason:Stacktrace ->
                {error, {Class, Reason, Stacktrace}}
        end,
    case Result of
        {ok, Released} ->
            ?tp(mq_cleanup_refreshed, #{
                id => Id, shard => Shard, released => Released, boundary => Boundary
            }),
            Failures > 0 andalso
                ?tp(info, mq_cleanup_recovered, #{id => Id, shard => Shard, failures => Failures}),
            case Owner of
                expired -> done;
                _ -> {continue, Item#{failures := 0}}
            end;
        {error, Error} ->
            log_failure(Failures, #{id => Id, shard => Shard, reason => Error}),
            Delay = retry_delay(Failures),
            {continue, start_timer(Key, Delay, Item#{failures := Failures + 1})}
    end.

%% Log the first failure of a series as a warning, and the repeated failures at debug level.
log_failure(0, Meta) ->
    ?tp(warning, mq_cleanup_retry, Meta);
log_failure(_Failures, Meta) ->
    ?tp(debug, mq_cleanup_retry, Meta).

retry_delay(Failures) ->
    Interval = ?MODULE:retry_interval(),
    min(Interval bsl min(Failures, 8), ?MAX_RETRY_FACTOR * Interval).

%% Start processing after the delay, unless a timer is already pending.
ensure_timer(_Key, #{timer := Timer} = Item) when Timer =/= undefined ->
    Item;
ensure_timer(Key, Item) ->
    start_timer(Key, ?MODULE:process_delay(), Item).

start_timer(Key, Delay, Item) ->
    Item#{timer := emqx_utils:start_timer(Delay, {work, Key})}.

%% Also remove the timeout message of a timer that already fired.
cancel_timer(#{timer := Timer}) ->
    emqx_utils:cancel_timer(Timer).
