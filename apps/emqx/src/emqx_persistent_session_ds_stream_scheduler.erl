%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_persistent_session_ds_stream_scheduler).

%% API:
-export([find_new_streams/1, find_replay_streams/1, is_fully_acked/2]).
-export([renew_streams/1, on_unsubscribe/2]).

%% behavior callbacks:
-export([]).

%% internal exports:
-export([]).

-export_type([]).

-include_lib("emqx/include/logger.hrl").
-include("emqx_mqtt.hrl").
-include("emqx_persistent_session_ds.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

%%================================================================================
%% API functions
%%================================================================================

%% @doc Find the streams that have uncommitted (in-flight) messages.
%% Return them in the order they were previously replayed.
-spec find_replay_streams(emqx_persistent_session_ds_state:t()) ->
    [{emqx_persistent_session_ds_state:stream_key(), emqx_persistent_session_ds:stream_state()}].
find_replay_streams(S) ->
    Comm1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
    Comm2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
    %% 1. Find the streams that aren't fully acked
    Streams = emqx_persistent_session_ds_state:fold_streams(
        fun(Key, Stream, Acc) ->
            case is_fully_acked(Comm1, Comm2, Stream) of
                false ->
                    [{Key, Stream} | Acc];
                true ->
                    Acc
            end
        end,
        [],
        S
    ),
    lists:sort(fun compare_streams/2, Streams).

%% @doc Find streams from which the new messages can be fetched.
%%
%% Currently it amounts to the streams that don't have any inflight
%% messages, since for performance reasons we keep only one record of
%% in-flight messages per stream, and we don't want to overwrite these
%% records prematurely.
%%
%% This function is non-detereministic: it randomizes the order of
%% streams to ensure fair replay of different topics.
-spec find_new_streams(emqx_persistent_session_ds_state:t()) ->
    [{emqx_persistent_session_ds_state:stream_key(), emqx_persistent_session_ds:stream_state()}].
find_new_streams(S) ->
    %% FIXME: this function is currently very sensitive to the
    %% consistency of the packet IDs on both broker and client side.
    %%
    %% If the client fails to properly ack packets due to a bug, or a
    %% network issue, or if the state of streams and seqno tables ever
    %% become de-synced, then this function will return an empty list,
    %% and the replay cannot progress.
    %%
    %% In other words, this function is not robust, and we should find
    %% some way to get the replays un-stuck at the cost of potentially
    %% losing messages during replay (or just kill the stuck channel
    %% after timeout?)
    Comm1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
    Comm2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
    shuffle(
        emqx_persistent_session_ds_state:fold_streams(
            fun
                (_Key, #srs{it_end = end_of_stream}, Acc) ->
                    Acc;
                (Key, Stream, Acc) ->
                    case is_fully_acked(Comm1, Comm2, Stream) andalso not Stream#srs.unsubscribed of
                        true ->
                            [{Key, Stream} | Acc];
                        false ->
                            Acc
                    end
            end,
            [],
            S
        )
    ).

%% @doc This function makes the session aware of the new streams.
%%
%% It has the following properties:
%%
%% 1. For each RankX, it keeps only the streams with the same RankY.
%%
%% 2. For each RankX, it never advances RankY until _all_ streams with
%% the same RankX are replayed.
%%
%% 3. Once all streams with the given rank are replayed, it advances
%% the RankY to the smallest known RankY that is greater than replayed
%% RankY.
%%
%% 4. If the RankX has never been replayed, it selects the streams
%% with the smallest RankY.
%%
%% This way, messages from the same topic/shard are never reordered.
-spec renew_streams(emqx_persistent_session_ds_state:t()) -> emqx_persistent_session_ds_state:t().
renew_streams(S0) ->
    S1 = remove_unsubscribed_streams(S0),
    S2 = remove_fully_replayed_streams(S1),
    emqx_persistent_session_ds_subs:fold(
        fun
            (Key, #{start_time := StartTime, id := SubId, deleted := false}, Acc) ->
                TopicFilter = emqx_topic:words(Key),
                Streams = select_streams(
                    SubId,
                    emqx_ds:get_streams(?PERSISTENT_MESSAGE_DB, TopicFilter, StartTime),
                    Acc
                ),
                lists:foldl(
                    fun(I, Acc1) ->
                        ensure_iterator(TopicFilter, StartTime, SubId, I, Acc1)
                    end,
                    Acc,
                    Streams
                );
            (_Key, _DeletedSubscription, Acc) ->
                Acc
        end,
        S2,
        S2
    ).

-spec on_unsubscribe(
    emqx_persistent_session_ds:subscription_id(), emqx_persistent_session_ds_state:t()
) ->
    emqx_persistent_session_ds_state:t().
on_unsubscribe(SubId, S0) ->
    %% NOTE: this function only marks the streams for deletion,
    %% instead of outright deleting them.
    %%
    %% It's done for two reasons:
    %%
    %% - MQTT standard states that the broker MUST process acks for
    %% all sent messages, and it MAY keep on sending buffered
    %% messages:
    %% https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901186
    %%
    %% - Deleting the streams may lead to gaps in the sequence number
    %% series, and lead to problems with acknowledgement tracking, we
    %% avoid that by delaying the deletion.
    %%
    %% When the stream is marked for deletion, the session won't fetch
    %% _new_ batches from it. Actual deletion is done by
    %% `renew_streams', when it detects that all in-flight messages
    %% from the stream have been acked by the client.
    emqx_persistent_session_ds_state:fold_streams(
        fun(Key, Srs, Acc) ->
            case Key of
                {SubId, _Stream} ->
                    %% This stream belongs to a deleted subscription.
                    %% Mark for deletion:
                    emqx_persistent_session_ds_state:put_stream(
                        Key, Srs#srs{unsubscribed = true}, Acc
                    );
                _ ->
                    Acc
            end
        end,
        S0,
        S0
    ).

-spec is_fully_acked(
    emqx_persistent_session_ds:stream_state(), emqx_persistent_session_ds_state:t()
) -> boolean().
is_fully_acked(Srs, S) ->
    CommQos1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
    CommQos2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
    is_fully_acked(CommQos1, CommQos2, Srs).

%%================================================================================
%% Internal functions
%%================================================================================

ensure_iterator(TopicFilter, StartTime, SubId, {{RankX, RankY}, Stream}, S) ->
    Key = {SubId, Stream},
    case emqx_persistent_session_ds_state:get_stream(Key, S) of
        undefined ->
            ?SLOG(debug, #{
                msg => new_stream, key => Key, stream => Stream
            }),
            {ok, Iterator} = emqx_ds:make_iterator(
                ?PERSISTENT_MESSAGE_DB, Stream, TopicFilter, StartTime
            ),
            NewStreamState = #srs{
                rank_x = RankX,
                rank_y = RankY,
                it_begin = Iterator,
                it_end = Iterator
            },
            emqx_persistent_session_ds_state:put_stream(Key, NewStreamState, S);
        #srs{} ->
            S
    end.

select_streams(SubId, Streams0, S) ->
    TopicStreamGroups = maps:groups_from_list(fun({{X, _}, _}) -> X end, Streams0),
    maps:fold(
        fun(RankX, Streams, Acc) ->
            select_streams(SubId, RankX, Streams, S) ++ Acc
        end,
        [],
        TopicStreamGroups
    ).

select_streams(SubId, RankX, Streams0, S) ->
    %% 1. Find the streams with the rank Y greater than the recorded one:
    Streams1 =
        case emqx_persistent_session_ds_state:get_rank({SubId, RankX}, S) of
            undefined ->
                Streams0;
            ReplayedY ->
                [I || I = {{_, Y}, _} <- Streams0, Y > ReplayedY]
        end,
    %% 2. Sort streams by rank Y:
    Streams = lists:sort(
        fun({{_, Y1}, _}, {{_, Y2}, _}) ->
            Y1 =< Y2
        end,
        Streams1
    ),
    %% 3. Select streams with the least rank Y:
    case Streams of
        [] ->
            [];
        [{{_, MinRankY}, _} | _] ->
            lists:takewhile(fun({{_, Y}, _}) -> Y =:= MinRankY end, Streams)
    end.

%% @doc Remove fully acked streams for the deleted subscriptions.
-spec remove_unsubscribed_streams(emqx_persistent_session_ds_state:t()) ->
    emqx_persistent_session_ds_state:t().
remove_unsubscribed_streams(S0) ->
    CommQos1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S0),
    CommQos2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S0),
    emqx_persistent_session_ds_state:fold_streams(
        fun(Key, ReplayState, S1) ->
            case
                ReplayState#srs.unsubscribed andalso is_fully_acked(CommQos1, CommQos2, ReplayState)
            of
                true ->
                    emqx_persistent_session_ds_state:del_stream(Key, S1);
                false ->
                    S1
            end
        end,
        S0,
        S0
    ).

%% @doc Advance RankY for each RankX that doesn't have any unreplayed
%% streams.
%%
%% Drop streams with the fully replayed rank. This function relies on
%% the fact that all streams with the same RankX have also the same
%% RankY.
-spec remove_fully_replayed_streams(emqx_persistent_session_ds_state:t()) ->
    emqx_persistent_session_ds_state:t().
remove_fully_replayed_streams(S0) ->
    CommQos1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S0),
    CommQos2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S0),
    %% 1. For each subscription, find the X ranks that were fully replayed:
    Groups = emqx_persistent_session_ds_state:fold_streams(
        fun({SubId, _Stream}, StreamState = #srs{rank_x = RankX, rank_y = RankY}, Acc) ->
            Key = {SubId, RankX},
            case {is_fully_replayed(CommQos1, CommQos2, StreamState), Acc} of
                {_, #{Key := false}} ->
                    Acc;
                {true, #{Key := {true, RankY}}} ->
                    Acc;
                {true, #{Key := {true, _RankYOther}}} ->
                    %% assert, should never happen
                    error(multiple_rank_y_for_rank_x);
                {true, #{}} ->
                    Acc#{Key => {true, RankY}};
                {false, #{}} ->
                    Acc#{Key => false}
            end
        end,
        #{},
        S0
    ),
    %% 2. Advance rank y for each fully replayed set of streams:
    S1 = maps:fold(
        fun
            (Key, {true, RankY}, Acc) ->
                emqx_persistent_session_ds_state:put_rank(Key, RankY, Acc);
            (_, _, Acc) ->
                Acc
        end,
        S0,
        Groups
    ),
    %% 3. Remove the fully replayed streams:
    emqx_persistent_session_ds_state:fold_streams(
        fun(Key = {SubId, _Stream}, #srs{rank_x = RankX, rank_y = RankY}, Acc) ->
            case emqx_persistent_session_ds_state:get_rank({SubId, RankX}, Acc) of
                undefined ->
                    Acc;
                MinRankY when RankY =< MinRankY ->
                    ?SLOG(debug, #{
                        msg => del_fully_preplayed_stream,
                        key => Key,
                        rank => {RankX, RankY},
                        min => MinRankY
                    }),
                    emqx_persistent_session_ds_state:del_stream(Key, Acc);
                _ ->
                    Acc
            end
        end,
        S1,
        S1
    ).

%% @doc Compare the streams by the order in which they were replayed.
compare_streams(
    {_KeyA, #srs{first_seqno_qos1 = A1, first_seqno_qos2 = A2}},
    {_KeyB, #srs{first_seqno_qos1 = B1, first_seqno_qos2 = B2}}
) ->
    case A1 =:= B1 of
        true ->
            A2 =< B2;
        false ->
            A1 < B1
    end.

is_fully_replayed(Comm1, Comm2, S = #srs{it_end = It}) ->
    It =:= end_of_stream andalso is_fully_acked(Comm1, Comm2, S).

is_fully_acked(_, _, #srs{
    first_seqno_qos1 = Q1, last_seqno_qos1 = Q1, first_seqno_qos2 = Q2, last_seqno_qos2 = Q2
}) ->
    %% Streams where the last chunk doesn't contain any QoS1 and 2
    %% messages are considered fully acked:
    true;
is_fully_acked(Comm1, Comm2, #srs{last_seqno_qos1 = S1, last_seqno_qos2 = S2}) ->
    (Comm1 >= S1) andalso (Comm2 >= S2).

-spec shuffle([A]) -> [A].
shuffle(L0) ->
    L1 = lists:map(
        fun(A) ->
            %% maybe topic/stream prioritization could be introduced here?
            {rand:uniform(), A}
        end,
        L0
    ),
    L2 = lists:sort(L1),
    {_, L} = lists:unzip(L2),
    L.
