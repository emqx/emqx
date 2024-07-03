%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_leader_rank_progress).

-include_lib("emqx/include/logger.hrl").

-export([
    init/0,
    set_replayed/2,
    add_streams/2
]).

%% "shard"
-type rank_x() :: emqx_ds:rank_x().

%% "generation"
-type rank_y() :: emqx_ds:rank_y().

%% shard progress
-type x_progress() :: #{
    %% All streams with given rank_x and rank_y =< min_y are replayed.
    min_y := rank_y(),

    ys := #{
        rank_y() => #{
            emqx_ds:stream() => _IdReplayed :: boolean()
        }
    }
}.

-type t() :: #{
    rank_x() => x_progress()
}.

-spec init() -> t().
init() -> #{}.

-spec set_replayed(emqx_ds:stream_rank(), t()) -> t().
set_replayed({{RankX, RankY}, Stream}, State) ->
    case State of
        #{RankX := #{ys := #{RankY := #{Stream := false} = RankYStreams} = Ys0}} ->
            Ys1 = Ys0#{RankY => RankYStreams#{Stream => true}},
            {MinY, Ys2} = update_min_y(maps:to_list(Ys1)),
            State#{RankX => #{min_y => MinY, ys => Ys2}};
        _ ->
            ?SLOG(
                warning,
                leader_rank_progress_double_or_invalid_update,
                #{
                    rank_x => RankX,
                    rank_y => RankY,
                    state => State
                }
            ),
            State
    end.

-spec add_streams([{emqx_ds:stream_rank(), emqx_ds:stream()}], t()) -> false | {true, t()}.
add_streams(StreamsWithRanks, State) ->
    SortedStreamsWithRanks = lists:sort(
        fun({{_RankX1, RankY1}, _Stream1}, {{_RankX2, RankY2}, _Stream2}) ->
            RankY1 =< RankY2
        end,
        StreamsWithRanks
    ),
    lists:foldl(
        fun({Rank, Stream} = StreamWithRank, {StreamAcc, StateAcc0}) ->
            case add_stream({Rank, Stream}, StateAcc0) of
                {true, StateAcc1} ->
                    {[StreamWithRank | StreamAcc], StateAcc1};
                false ->
                    {StreamAcc, StateAcc0}
            end
        end,
        {[], State},
        SortedStreamsWithRanks
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

add_stream({{RankX, RankY}, Stream}, State0) ->
    case State0 of
        #{RankX := #{min_y := MinY}} when RankY =< MinY ->
            false;
        #{RankX := #{ys := #{RankY := #{Stream := true}}}} ->
            false;
        _ ->
            XProgress = maps:get(RankX, State0, #{min_y => RankY - 1, ys => #{}}),
            Ys0 = maps:get(ys, XProgress),
            RankYStreams0 = maps:get(RankY, Ys0, #{}),
            RankYStreams1 = RankYStreams0#{Stream => false},
            Ys1 = Ys0#{RankY => RankYStreams1},
            State1 = State0#{RankX => XProgress#{ys => Ys1}},
            {true, State1}
    end.

update_min_y([{RankY, RankYStreams} | Rest] = Ys) ->
    case {has_unreplayed_streams(RankYStreams), Rest} of
        {true, _} ->
            {RankY, maps:from_list(Ys)};
        {false, []} ->
            {RankY - 1, #{}};
        {false, _} ->
            update_min_y(Rest)
    end.

has_unreplayed_streams(RankYStreams) ->
    lists:any(
        fun(IsReplayed) -> not IsReplayed end,
        maps:values(RankYStreams)
    ).
