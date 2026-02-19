%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_username_quota_snapshot).

-moduledoc """
Snapshot owner process for paginated username-quota listing.

This server keeps an ETS snapshot sorted by `{used, username}` and serves pages by
cursor. Cursors encode `{node, generation, last_key}` so follow-up requests can be
routed back to the same node and generation.

`rebuilding_snapshot` is returned when a request's `DeadlineMs` is already exhausted
or becomes exhausted while a snapshot rebuild is in progress. This is expected for
stale requests, for example when clients retry late with an old cursor/deadline while
the owner is refreshing the snapshot.
""".

-behaviour(gen_server).

-export([
    start_link/0,
    request_page/4,
    request_page_local/4,
    reset/0
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include("emqx_username_quota.hrl").

-define(SERVER, ?MODULE).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

request_page(RequesterPid, DeadlineMs, Cursor, Limit) ->
    case maybe_forward_request(RequesterPid, DeadlineMs, Cursor, Limit) of
        local ->
            request_page_local(RequesterPid, DeadlineMs, Cursor, Limit);
        Result ->
            Result
    end.

request_page_local(RequesterPid, DeadlineMs, Cursor, Limit) ->
    TimeoutMs = erlang:max(1, DeadlineMs - now_ms()),
    try
        gen_server:call(
            ?SERVER,
            {request_page, RequesterPid, DeadlineMs, Cursor, Limit},
            TimeoutMs
        )
    catch
        exit:{timeout, _} ->
            {error, {busy, pseudo_cursor(node())}}
    end.

reset() ->
    try gen_server:call(?SERVER, reset, infinity) of
        ok -> ok
    catch
        exit:_ ->
            ok
    end.

init([]) ->
    ensure_snapshot_table(),
    {ok, #{last_refresh_ms => 0, generation => 0, taken_at_ms => 0}}.

handle_call(reset, _From, State) ->
    ok = clear_snapshot_table(),
    {reply, ok, State#{last_refresh_ms => 0, generation => 0, taken_at_ms => 0}};
handle_call(
    {request_page, _RequesterPid, DeadlineMs, Cursor, Limit},
    _From,
    State0
) ->
    case DeadlineMs =< now_ms() of
        true ->
            {reply, {error, {rebuilding_snapshot, pseudo_cursor(node())}}, State0};
        false ->
            State = maybe_refresh_snapshot(State0),
            case DeadlineMs =< now_ms() of
                true ->
                    {reply, {error, {rebuilding_snapshot, pseudo_cursor(node())}}, State};
                false ->
                    {reply, {ok, page_snapshot(State, Cursor, Limit)}, State}
            end
    end;
handle_call(_Req, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

maybe_refresh_snapshot(State = #{last_refresh_ms := LastRefresh}) ->
    RefreshIntervalMs = emqx_username_quota_config:snapshot_refresh_interval_ms(),
    NeedRefresh = LastRefresh =:= 0 orelse now_ms() - LastRefresh >= RefreshIntervalMs,
    case NeedRefresh of
        true ->
            rebuild_snapshot(),
            Now = now_ms(),
            Generation = maps:get(generation, State, 0) + 1,
            State#{last_refresh_ms => Now, generation => Generation, taken_at_ms => Now};
        false ->
            State
    end.

rebuild_snapshot() ->
    ok = clear_snapshot_table(),
    ok = emqx_username_quota_state:fold_username_counts(
        fun(Username, Counter, ok) ->
            _ = ets:insert(?SNAPSHOT_TAB, {{Counter, Username}, Counter}),
            ok
        end,
        ok
    ),
    ok.

page_snapshot(State, Cursor, Limit) ->
    Generation = maps:get(generation, State, 0),
    Total = ets:info(?SNAPSHOT_TAB, size),
    StartKey = resolve_start_key(Cursor, Generation),
    {Rows, LastKey, HasMore} = collect_page(StartKey, Limit, [], undefined),
    NextCursor =
        case HasMore andalso LastKey =/= undefined of
            true -> encode_cursor(node(), Generation, LastKey);
            false -> undefined
        end,
    #{
        total => Total,
        data => Rows,
        snapshot => #{
            node => node(),
            generation => Generation,
            taken_at_ms => maps:get(taken_at_ms, State, 0)
        },
        next_cursor => NextCursor
    }.

ensure_snapshot_table() ->
    _ = ets:new(?SNAPSHOT_TAB, [ordered_set, named_table, public]),
    ok.

now_ms() ->
    erlang:system_time(millisecond).

clear_snapshot_table() ->
    true = ets:delete_all_objects(?SNAPSHOT_TAB),
    ok.

maybe_forward_request(RequesterPid, DeadlineMs, Cursor, Limit) ->
    case decode_cursor(Cursor) of
        {ok, {RemoteNode, _Generation, _LastKey}} when RemoteNode =/= node() ->
            TimeoutMs = erlang:max(1, DeadlineMs - now_ms()),
            try
                gen_server:call(
                    {?SERVER, RemoteNode},
                    {request_page, RequesterPid, DeadlineMs, Cursor, Limit},
                    TimeoutMs
                )
            catch
                exit:{timeout, _} ->
                    local
            end;
        _ ->
            local
    end.

resolve_start_key(Cursor, Generation) ->
    case decode_cursor(Cursor) of
        {ok, {CursorNode, CursorGeneration, LastKey}} when
            CursorNode =:= node() andalso CursorGeneration =:= Generation
        ->
            ets:next(?SNAPSHOT_TAB, LastKey);
        _ ->
            ets:first(?SNAPSHOT_TAB)
    end.

collect_page(Key, 0, Acc, LastKey) ->
    {lists:reverse(Acc), LastKey, Key =/= '$end_of_table'};
collect_page('$end_of_table', _Limit, Acc, LastKey) ->
    {lists:reverse(Acc), LastKey, false};
collect_page({Counter, _Username} = Key, Limit, Acc, _LastKey) ->
    Row = {Key, Counter},
    collect_page(ets:next(?SNAPSHOT_TAB, Key), Limit - 1, [Row | Acc], Key).

encode_cursor(Node, Generation, LastKey) ->
    base64:encode(term_to_binary({Node, Generation, LastKey})).

decode_cursor(undefined) ->
    error;
decode_cursor(Cursor) when is_list(Cursor) ->
    decode_cursor(iolist_to_binary(Cursor));
decode_cursor(Cursor) when is_binary(Cursor) ->
    try binary_to_term(base64:decode(Cursor), [safe]) of
        {Node, Generation, {Counter, Username} = LastKey} when
            is_atom(Node) andalso
                is_integer(Generation) andalso
                is_integer(Counter) andalso
                is_binary(Username)
        ->
            {ok, {Node, Generation, LastKey}};
        _ ->
            error
    catch
        _:_ ->
            error
    end;
decode_cursor(_Other) ->
    error.

pseudo_cursor(Node) ->
    encode_cursor(Node, 0, {0, <<>>}).
