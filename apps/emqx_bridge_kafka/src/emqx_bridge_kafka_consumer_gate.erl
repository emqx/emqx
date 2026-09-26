%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kafka_consumer_gate).

-moduledoc """
Starts Kafka group subscribers once the node is ready.

A subscriber started while `emqx_node_readiness:is_ready/0` returns `false`
stays in `emqx_bridge_kafka_consumer_sup` without a process, so it neither
joins its consumer group nor commits offsets.  This server restarts such
subscribers when the node is ready, and retries the ones that fail to start.
""".

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([start_link/0, start_subscriber/1]).

%% `gen_server' API
-export([init/1, handle_continue/2, handle_call/3, handle_cast/2, handle_info/2]).

-define(POLL_INTERVAL, 100).
-ifdef(TEST).
-define(RETRY_INTERVAL, 500).
-else.
-define(RETRY_INTERVAL, 5_000).
-endif.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-doc "Start function of the group subscriber child in `emqx_bridge_kafka_consumer_sup`.".
-spec start_subscriber(map()) -> {ok, pid()} | ignore | {error, term()}.
start_subscriber(GroupSubscriberConfig) ->
    case emqx_node_readiness:is_ready() of
        true ->
            brod_group_subscriber_v2:start_link(GroupSubscriberConfig);
        false ->
            gen_server:cast(?MODULE, poll),
            ignore
    end.

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init([]) ->
    {ok, idle, {continue, scan}}.

%% Picks up subscribers left waiting when this server restarts.
handle_continue(scan, idle) ->
    case waiting_subscriber_ids() of
        [] -> {noreply, idle};
        [_ | _] -> {noreply, schedule(0)}
    end.

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(poll, idle) ->
    {noreply, schedule(0)};
handle_cast(poll, armed) ->
    {noreply, armed};
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info(poll, armed) ->
    case emqx_node_readiness:is_ready() of
        true ->
            case start_waiting_subscribers() of
                ok -> {noreply, idle};
                retry -> {noreply, schedule(?RETRY_INTERVAL)}
            end;
        false ->
            {noreply, schedule(?POLL_INTERVAL)}
    end;
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

schedule(Timeout) ->
    _ = erlang:send_after(Timeout, self(), poll),
    armed.

waiting_subscriber_ids() ->
    [
        Id
     || {Id, undefined, _, _} <- supervisor:which_children(emqx_bridge_kafka_consumer_sup)
    ].

start_waiting_subscribers() ->
    Ids = waiting_subscriber_ids(),
    Results = lists:map(fun restart_subscriber/1, Ids),
    ?tp(kafka_consumer_gate_scan_done, #{subscriber_ids => Ids}),
    case lists:member(retry, Results) of
        true -> retry;
        false -> ok
    end.

restart_subscriber(Id) ->
    case brod_supervisor3:restart_child(emqx_bridge_kafka_consumer_sup, Id) of
        {ok, Pid} when is_pid(Pid) ->
            ?tp(info, "kafka_consumer_subscriber_started_after_node_ready", #{subscriber_id => Id}),
            ok;
        {ok, undefined} ->
            %% The node is not ready again; `start_subscriber/1' cast the next poll.
            ok;
        {error, Reason} when
            Reason =:= not_found; Reason =:= running; Reason =:= restarting
        ->
            %% Its source was removed, re-added or restarted after `which_children'.
            ok;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_start_kafka_subscriber",
                subscriber_id => Id,
                reason => emqx_utils:redact(Reason)
            }),
            retry
    end.
