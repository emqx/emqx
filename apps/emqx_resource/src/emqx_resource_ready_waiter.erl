%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_resource_ready_waiter).

-moduledoc """
Runs the source starts deferred until `emqx_node_readiness:is_ready/0` returns
`true`.

A source that must not consume while the node is booting calls `when_ready/2`
at its start point. The deferred starts are kept in a table owned by
`emqx_resource_sup`, so they survive a restart of this server.
""".

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%% API
-export([cancel/1, create_table/0, start_link/0, when_ready/2]).

%% `gen_server' API
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(TAB, ?MODULE).
-define(POLL_INTERVAL, 100).
-ifdef(TEST).
-define(RETRY_INTERVAL, 500).
-else.
-define(RETRY_INTERVAL, 5_000).
-endif.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-doc """
Returns `now` if the node is ready, and the caller starts inline.

Otherwise stores `MFA` under `Key`, replacing any earlier entry with that key,
and returns `deferred`; the waiter applies it once the node is ready. The
deferred starts run one after another, so `MFA` must return `ok` or
`{error, _}` promptly and must not wait on the network. It must be safe to
apply twice. An `{error, _}` or an exception is retried every 5 seconds.

`Key` is `{CallerModule, Id}` and names the entry in logs.
""".
-spec when_ready(term(), {module(), atom(), [term()]}) -> now | deferred.
when_ready(Key, {M, F, A} = MFA) when is_atom(M), is_atom(F), is_list(A) ->
    case emqx_node_readiness:is_ready() of
        true ->
            now;
        false ->
            true = ets:insert(?TAB, {Key, MFA, make_ref()}),
            %% A cast: callers include supervisors this server calls into.
            gen_server:cast(?MODULE, wake),
            deferred
    end.

-doc """
Drops the start deferred under `Key`, if any. A start the waiter has already
begun to apply still completes.
""".
-spec cancel(term()) -> ok.
cancel(Key) ->
    true = ets:delete(?TAB, Key),
    ok.

-doc "Creates the table of deferred starts, owned by the calling process.".
-spec create_table() -> ok.
create_table() ->
    emqx_utils_ets:new(?TAB, [set, public]).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init([]) ->
    {ok, wake(idle)}.

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(wake, State) ->
    {noreply, wake(State)};
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info(poll, armed) ->
    {noreply, poll()};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

wake(idle) ->
    case ets:info(?TAB, size) of
        0 -> idle;
        _ -> schedule(0)
    end;
wake(armed) ->
    armed.

poll() ->
    case ets:info(?TAB, size) of
        0 ->
            idle;
        _ ->
            case emqx_node_readiness:is_ready() of
                false ->
                    schedule(?POLL_INTERVAL);
                true ->
                    case run_all() of
                        ok -> idle;
                        retry -> schedule(?RETRY_INTERVAL)
                    end
            end
    end.

schedule(Timeout) ->
    _ = erlang:send_after(Timeout, self(), poll),
    armed.

run_all() ->
    Results = lists:map(fun run/1, ets:tab2list(?TAB)),
    ?tp(resource_ready_waiter_ran, #{keys => [Key || {Key, Result} <- Results, Result =/= skipped]}),
    case lists:keymember(retry, 2, Results) of
        true -> retry;
        false -> ok
    end.

%% An entry cancelled or replaced since `run_all/0' read the table is skipped.
run({Key, _MFA, _Ref} = Entry) ->
    case ets:lookup(?TAB, Key) of
        [Entry] -> {Key, apply_start(Entry)};
        _ -> {Key, skipped}
    end.

%% `delete_object' keeps an entry registered again under the same key while the
%% start ran.
apply_start({Key, {M, F, A}, _Ref} = Entry) ->
    try apply(M, F, A) of
        ok ->
            true = ets:delete_object(?TAB, Entry),
            ok;
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "deferred_start_failed",
                key => Key,
                reason => emqx_utils:redact(Reason)
            }),
            retry;
        Other ->
            ?SLOG(error, #{
                msg => "deferred_start_bad_return",
                key => Key,
                return => emqx_utils:redact(Other)
            }),
            true = ets:delete_object(?TAB, Entry),
            ok
    catch
        Class:Reason:Stacktrace ->
            ?SLOG(error, #{
                msg => "deferred_start_crashed",
                key => Key,
                exception => Class,
                reason => emqx_utils:redact(Reason),
                stacktrace => emqx_utils:redact(Stacktrace)
            }),
            retry
    end.
