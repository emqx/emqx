%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_pmon).

-compile({no_auto_import, [monitor/3, demonitor/1, demonitor/2]}).

-export([new/0]).

-export([
    monitor/2,
    monitor/3,
    demonitor/1,
    demonitor/2
]).

-export([
    find/2,
    erase/2,
    erase_all/2
]).

-export([count/1]).

-export_type([pmon/0]).

-opaque pmon() :: {?MODULE, map()}.

-define(PMON(Map), {?MODULE, Map}).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec new() -> pmon().
new() -> ?PMON(maps:new()).

-spec monitor(pid(), pmon()) -> pmon().
monitor(Pid, PMon) ->
    ?MODULE:monitor(Pid, undefined, PMon).

-spec monitor(pid(), term(), pmon()) -> pmon().
monitor(Pid, Val, PMon = ?PMON(Map)) ->
    case maps:is_key(Pid, Map) of
        true ->
            PMon;
        false ->
            Ref = erlang:monitor(process, Pid),
            ?PMON(maps:put(Pid, {Ref, Val}, Map))
    end.

-spec demonitor(pid(), pmon()) -> pmon().
demonitor(Pid, PMon = ?PMON(Map)) ->
    case maps:find(Pid, Map) of
        {ok, {Ref, _Val}} ->
            ok = demonitor(Ref),
            ?PMON(maps:remove(Pid, Map));
        error ->
            PMon
    end.

-spec demonitor(reference()) -> ok.
demonitor(Ref) when is_reference(Ref) ->
    _ = erlang:demonitor(Ref, [flush]),
    ok.

-spec find(pid(), pmon()) -> error | {ok, term()}.
find(Pid, ?PMON(Map)) ->
    case maps:find(Pid, Map) of
        {ok, {_Ref, Val}} ->
            {ok, Val};
        error ->
            error
    end.

-spec erase(pid(), pmon()) -> pmon().
erase(Pid, ?PMON(Map)) ->
    ?PMON(maps:remove(Pid, Map)).

-spec erase_all([pid()], pmon()) -> {[{pid(), term()}], pmon()}.
erase_all(Pids, PMon0) ->
    lists:foldl(
        fun(Pid, {Acc, PMon}) ->
            case find(Pid, PMon) of
                {ok, Val} ->
                    {[{Pid, Val} | Acc], erase(Pid, PMon)};
                error ->
                    {Acc, PMon}
            end
        end,
        {[], PMon0},
        Pids
    ).

-spec count(pmon()) -> non_neg_integer().
count(?PMON(Map)) -> maps:size(Map).
