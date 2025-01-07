%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc EMQX DS eXplorer. A simple DS client process that can be used
%% to inspect durable messages in the EMQX console or for
%% benchmarking.
-module(emqx_dsx).

-behaviour(gen_server).

%% API:
-export([start_link/1, ls/0, stop/1, more/1]).
%% Convenience API:
-export([c/0, c/1, stop/0, more/0]).
%% Callback definitions:
-export([print/0, null/0, stats/1, create_stats_worker/1]).

%% behaviour callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([]).

-export_type([]).

-include("emqx_ds.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("kernel/include/logger.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(name(REC), {n, l, {?MODULE, REC}}).
-define(via(REC), {via, gproc, ?name(REC)}).

-record(more_req, {}).

-type callback() :: fun(
    (emqx_ds:stream(), [{emqx_ds:message_key(), emqx_types:message()}], State | undefined) -> State
).

%%================================================================================
%% API functions
%%================================================================================

%%%% Print callback
-spec print() -> callback().
print() ->
    fun(Stream, Msgs, _) ->
        lists:foreach(
            fun({Key, Msg}) ->
                io:format(user, "~w/~w:~n   ~p~n", [Stream, Key, emqx_message:to_map(Msg)])
            end,
            Msgs
        )
    end.

%%%% Null callback
-spec null() -> callback().
null() ->
    fun(_, _, _) ->
        undefined
    end.

%%%% Stats callback
%%
%% @doc How to use this module in benchmark mode:
%%
%% 1. Start statistics server (`create_stats_worker(foo)')
%%
%% 2. Create clients with stats callback, with name of the worker passed:
%% `emqx_dsx:start_link(#{topic => ..., callback => stats(foo)})'
%%
%% 3. Get stats by calling
%% `emqx_metrics_worker:get_metrics(foo, foo).'.
create_stats_worker(Name) ->
    {ok, _} = emqx_metrics_worker:start_link(Name),
    Metrics = [{counter, n_batches}, {counter, n_msgs}, {counter, n_bytes}, {slide, lag}],
    ok = emqx_metrics_worker:create_metrics(Name, Name, Metrics).

-spec stats(emqx_metrics_worker:handler_name()) -> callback().
stats(Worker) ->
    fun(_Stream, Msgs, _) ->
        emqx_metrics_worker:inc(Worker, Worker, n_batches),
        lists:foreach(fun(Msg) -> update_stats(Worker, Msg) end, Msgs)
    end.

update_stats(Worker, {_Key, Msg}) ->
    emqx_metrics_worker:inc(Worker, Worker, n_msgs),
    emqx_metrics_worker:inc(Worker, Worker, n_bytes, emqx_message:estimate_size(Msg)),
    Lag = emqx_message:timestamp_now() - Msg#message.timestamp,
    emqx_metrics_worker:observe(Worker, Worker, lag, Lag).

%%%%%
-spec start_link(#{
    topic := string() | [emqx_types:word()],
    name => term(),
    db => emqx_ds:db(),
    callback => callback(),
    active => true | pos_integer(),
    start => integer(),
    renew_streams_interval => pos_integer(),
    poll_timeout => pos_integer()
}) -> {ok, pid()}.
start_link(User0) ->
    User = maps:update_with(
        topic,
        fun(Topic) ->
            case Topic of
                _ when is_binary(Topic) ->
                    emqx_topic:words(Topic);
                [A | _] when is_atom(A); is_binary(A) ->
                    Topic;
                [A | _] when is_integer(A) ->
                    emqx_topic:words(list_to_binary(Topic))
            end
        end,
        User0
    ),
    Defaults = #{
        name => self(),
        db => messages,
        callback => print(),
        active => true,
        start => erlang:system_time(millisecond),
        renew_streams_interval => 5_000,
        poll_timeout => 30_000
    },
    Conf = #{name := Name} = maps:merge(Defaults, User),
    c(Name),
    gen_server:start_link(?via(Name), ?MODULE, Conf, []).

stop(Name) ->
    exit(gproc:where(?name(Name)), normal).

stop() ->
    stop(c()).

-spec c() -> _Name.
c() ->
    case get(?MODULE) of
        Pid when is_pid(Pid) ->
            Pid;
        undefined ->
            error(noproc)
    end.

-spec c(Name) -> Name | undefined.
c(Name) ->
    put(?MODULE, Name).

-spec ls() -> [{_Name, pid()}].
ls() ->
    MS = {{?name('$1'), '$2', '_'}, [], [{{'$1', '$2'}}]},
    gproc:select({local, names}, [MS]).

more(Name) ->
    gen_server:call(?via(Name), #more_req{}).

more() ->
    more(c()).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s, {
    callback :: callback(),
    callback_state :: _,
    db :: emqx_ds:db(),
    poll_timeout :: pos_integer(),
    topic :: emqx_ds:topic_filter(),
    start :: integer(),
    active :: true | pos_integer(),
    its = #{} :: #{emqx_ds:stream() => emqx_ds:iterator()},
    %% Streams that have been fully replayed and should be ignored:
    eos = #{} :: #{emqx_ds:stream() => _},
    renew_streams_interval :: pos_integer(),
    %%   Poll-related fields:
    %% In-progress poll requests:
    polls = #{} :: #{emqx_ds:stream() => reference()},
    %% Queue of pollable streams:
    pq :: queue:queue(emqx_ds:stream()),
    %% Number of streams with currently inflight poll requests:
    inflight = 0 :: non_neg_integer(),
    %% Number of poll requests sent to the DS so far, or since the
    %% last `next()' call:
    poll_count = 0 :: non_neg_integer()
}).

init(
    #{
        callback := Callback,
        active := Active,
        topic := TF,
        start := Start,
        db := DB,
        renew_streams_interval := RSI,
        poll_timeout := PollTimeout
    }
) ->
    process_flag(trap_exit, true),
    S = #s{
        callback = Callback,
        db = DB,
        topic = TF,
        start = Start,
        active = Active,
        renew_streams_interval = RSI,
        poll_timeout = PollTimeout,
        pq = queue:new()
    },
    {ok, poll(renew_streams(S))}.

handle_call(#more_req{}, _From, S0) ->
    S = S0#s{poll_count = 0},
    {reply, ok, poll(S)};
handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info(renew_streams, S) ->
    {noreply, renew_streams(S)};
handle_info(#poll_reply{ref = Ref, payload = poll_timeout}, S0 = #s{polls = Polls}) ->
    unalias(Ref),
    S = maps:fold(
        fun(Stream, R, S1) ->
            case R of
                Ref ->
                    handle_poll_timeout(Stream, S1);
                _ ->
                    S1
            end
        end,
        S0,
        Polls
    ),
    {noreply, poll(S)};
handle_info(
    #poll_reply{ref = Ref, userdata = Stream, payload = Payload},
    #s{inflight = Inflight, polls = Polls0} = S0
) ->
    case maps:take(Stream, Polls0) of
        {Ref, Polls} ->
            S = handle_poll_reply(Stream, Payload, S0#s{inflight = Inflight - 1, polls = Polls}),
            {noreply, poll(S)};
        _ ->
            logger:error("Stray poll reply ~p", [
                #{ref => Ref, stream => Stream, payload => Payload}
            ]),
            {noreply, S0}
    end;
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

handle_poll_timeout(Stream, S = #s{polls = Polls, inflight = Inflight, pq = PQ}) ->
    S#s{
        polls = maps:remove(Stream, Polls),
        inflight = Inflight - 1,
        pq = queue:in(Stream, PQ)
    }.

handle_poll_reply(
    Stream,
    {ok, Iterator, Messages},
    S = #s{its = Its0, pq = PQ0, callback = Callback, callback_state = CS0}
) ->
    CS = Callback(Stream, Messages, CS0),
    S#s{
        its = Its0#{Stream => Iterator},
        pq = queue:in(Stream, PQ0),
        callback_state = CS
    };
handle_poll_reply(Stream, {error, recoverable, _Err}, S = #s{pq = PQ0}) ->
    S#s{pq = queue:in(Stream, PQ0)};
handle_poll_reply(Stream, EOSEvent, S = #s{eos = EOS, db = DB}) ->
    case EOSEvent of
        {ok, end_of_stream} ->
            ok;
        {error, unrecoverable, Err} ->
            ?LOG_ERROR(#{
                msg => "Unrecoverable stream poll error", stream => Stream, error => Err, db => DB
            }),
            ok
    end,
    S#s{eos = EOS#{Stream => true}}.

renew_streams(
    S = #s{
        db = DB,
        topic = TF,
        start = Start,
        its = Its0,
        eos = EOS,
        renew_streams_interval = RSI,
        pq = PQ0
    }
) ->
    Streams = emqx_ds:get_streams(DB, TF, Start),
    {Its, PQ} = lists:foldl(
        fun({_Rank, Stream}, Acc = {Its1, PQ1}) ->
            case Its1 of
                #{Stream := _} ->
                    %% Stream already exists:
                    Acc;
                #{} ->
                    case
                        maps:is_key(Stream, EOS) orelse emqx_ds:make_iterator(DB, Stream, TF, Start)
                    of
                        {ok, It} ->
                            %% This is a new stream:
                            {Its1#{Stream => It}, queue:in(Stream, PQ1)};
                        _ ->
                            Acc
                    end
            end
        end,
        {Its0, PQ0},
        Streams
    ),
    erlang:send_after(RSI, self(), renew_streams),
    S#s{its = Its, pq = PQ}.

poll(S0) ->
    poll(S0, []).

poll(S0 = #s{inflight = Inflight, poll_count = PC}, PollStreams) ->
    case grab_more(S0) of
        {{value, Stream}, PQ} ->
            S = S0#s{pq = PQ, inflight = Inflight + 1, poll_count = PC + 1},
            poll(S, [Stream | PollStreams]);
        _ ->
            case PollStreams of
                [] ->
                    %% Nothing to poll:
                    S0;
                _ ->
                    do_poll(S0, PollStreams)
            end
    end.

grab_more(#s{active = Active, pq = PQ, poll_count = PC}) ->
    case Active of
        true ->
            queue:out(PQ);
        N when PC < N ->
            queue:out(PQ);
        _ ->
            false
    end.

do_poll(S = #s{db = DB, poll_timeout = Timeout, polls = Polls0, its = Its}, PollStreams) ->
    Req = [{Stream, maps:get(Stream, Its)} || Stream <- PollStreams],
    {ok, Ref} = emqx_ds:poll(DB, Req, #{timeout => Timeout}),
    NewPolls = maps:from_keys(PollStreams, Ref),
    Polls = maps:merge(Polls0, NewPolls),
    S#s{polls = Polls}.
