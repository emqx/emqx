%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_cleanup_worker).
-behaviour(gen_server).

-moduledoc """
Release quota of consumed records of a reject queue.

The consumer sends the latest consumed boundary of each shard.
The cleaner caches the next index boundary that can retire a bucket.
It runs an index transaction only when that boundary is unknown or the consumed boundary
reaches it.
""".

-include("emqx_mq_internal.hrl").

-export([child_spec/1, start_link/1, consumed/3]).

%% Consumption progress of a queue shard.
-record(mq_cleanup, {
    shard :: emqx_ds:shard(),
    %% Exclusive DS timestamp boundary. Every record of the shard below it is consumed.
    boundary :: non_neg_integer()
}).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

%% For testing
-export([retry_interval/0]).

-define(RETRY_INTERVAL, 1000).
%% Refreshes of an index without an older bucket to retire start at the retry interval.
%% The delay doubles after each refresh. Refreshes stop when the delay would exceed
%% this many retry intervals.
-define(MAX_REFRESH_DELAY_FACTOR, 16).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-doc "Return the child spec of the cleaner in the per-queue supervisor.".
-spec child_spec(emqx_mq_types:mq()) -> supervisor:child_spec().
child_spec(MQ) ->
    #{
        id => ?MQ_CLEANER_CHILD_ID,
        start => {?MODULE, start_link, [MQ]},
        restart => permanent,
        shutdown => brutal_kill
    }.

-spec start_link(emqx_mq_types:mq()) -> gen_server:start_ret().
start_link(MQ) ->
    gen_server:start_link(?MODULE, MQ, []).

-doc "Report that every record of the shard below the boundary is consumed.".
-spec consumed(pid(), emqx_ds:shard(), non_neg_integer()) -> ok.
consumed(Cleaner, Shard, Boundary) ->
    ?tp_debug(mq_cleanup_consumed, #{cleaner => Cleaner, shard => Shard, boundary => Boundary}),
    Cleaner ! #mq_cleanup{shard = Shard, boundary = Boundary},
    ok.

-spec retry_interval() -> non_neg_integer().
retry_interval() ->
    ?RETRY_INTERVAL.

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init(#{id := Id} = MQ) ->
    case emqx_mq_consumer:find(Id) of
        {ok, Consumer} ->
            Consumer ! {mq_cleanup_attach, self()},
            {ok, #{mq => MQ, shards => #{}}};
        not_found ->
            ignore
    end.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(#mq_cleanup{shard = Shard, boundary = Boundary}, #{shards := Shards0} = State) ->
    Work0 =
        case Shards0 of
            #{Shard := #{latest := Latest} = Existing} ->
                Existing#{
                    latest := max(Latest, Boundary), refresh_delay := ?MODULE:retry_interval()
                };
            #{} ->
                #{
                    latest => Boundary,
                    next => undefined,
                    delete => 0,
                    refresh_delay => ?MODULE:retry_interval(),
                    timer => undefined
                }
        end,
    Work =
        case need_index_work(Work0) of
            %% New consumption progress does not wait for a refresh or retry backoff
            true -> ensure_timer(Shard, 0, cancel_timer(Work0));
            false -> Work0
        end,
    {noreply, State#{shards := Shards0#{Shard => Work}}};
handle_info({timeout, TRef, {work, Shard}}, #{shards := Shards0} = State) ->
    #{Shard := #{timer := TRef} = Work0} = Shards0,
    Work = work(Shard, Work0#{timer := undefined}, State),
    {noreply, State#{shards := Shards0#{Shard := Work}}};
handle_info(_Message, State) ->
    ?tp_debug(mq_cleanup_unknown_message, #{message => _Message}),
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

%% `CutoffUs` comes from `truncate_index/3`. See "Deletion boundaries" in `emqx_mq_message_db`.
work(Shard, #{delete := CutoffUs} = Work, #{mq := MQ}) when CutoffUs > 0 ->
    case emqx_mq_message_db:delete_data_before(MQ, Shard, CutoffUs) of
        ok ->
            ok;
        {error, Reason} ->
            ?tp(warning, mq_cleanup_delete_failed, #{
                shard => Shard, cutoff_us => CutoffUs, reason => Reason
            })
    end,
    ensure_timer(Shard, 0, Work#{delete := 0});
work(Shard, #{latest := Boundary} = Work0, #{mq := MQ}) ->
    case emqx_mq_message_db:truncate_index(MQ, Shard, Boundary) of
        {ok, CutoffUs, Next} ->
            Work1 = Work0#{delete := CutoffUs, next := Next},
            {Pending, Work} =
                case {CutoffUs, Next} of
                    {0, undefined} -> refresh_later(Shard, Work1);
                    {0, _} -> {false, Work1};
                    _ -> {true, ensure_timer(Shard, 0, Work1)}
                end,
            ?tp(mq_cleanup_refreshed, #{
                shard => Shard, cutoff_us => CutoffUs, next => Next, pending => Pending
            }),
            Work;
        {error, Reason} ->
            retry(Shard, Work0, Reason)
    end.

%% A cached next boundary above the consumed boundary means no bucket can retire yet.
need_index_work(#{next := undefined}) ->
    true;
need_index_work(#{latest := Latest, next := Next}) ->
    Latest >= Next.

refresh_later(Shard, #{refresh_delay := Delay} = Work) ->
    case Delay =< ?MAX_REFRESH_DELAY_FACTOR * ?MODULE:retry_interval() of
        true -> {true, ensure_timer(Shard, Delay, Work#{refresh_delay := 2 * Delay})};
        false -> {false, Work}
    end.

retry(Shard, Work, Reason) ->
    ?tp(warning, mq_cleanup_retry, #{shard => Shard, reason => Reason}),
    ensure_timer(Shard, ?MODULE:retry_interval(), Work).

%% Cancel any pending timer before starting a new one.
ensure_timer(Shard, Delay, #{timer := Timer} = Work) ->
    ok = emqx_utils:cancel_timer(Timer),
    Work#{timer := emqx_utils:start_timer(Delay, {work, Shard})}.

%% Also remove the timeout message of a timer that already fired.
cancel_timer(#{timer := TRef} = Work) ->
    ok = emqx_utils:cancel_timer(TRef),
    Work#{timer := undefined}.
