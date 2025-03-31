%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(prop_emqx_retainer_index).

-include_lib("proper/include/proper.hrl").

-define(CHARS, 6).
-define(MAX_TOPIC_LEN, 12).
-define(MAX_INDEX_LEN, 4).
-define(MAX_FILTER_LEN, 6).

%%--------------------------------------------------------------------
%% Properties
%%--------------------------------------------------------------------

prop_index() ->
    ?FORALL(
        {Index, Topics0, Filter},
        {index_t(), list(topic_t()), filter_t()},
        begin
            Topics = lists:usort(Topics0),

            MatchedTopicsDirectly = lists:filter(
                fun(Topic) ->
                    emqx_topic:match(Topic, Filter)
                end,
                Topics
            ),

            Tab = ets:new(?MODULE, [set]),
            ok = lists:foreach(
                fun(Topic) ->
                    Key = emqx_retainer_index:to_index_key(Index, Topic),
                    ets:insert(Tab, {Key, true})
                end,
                Topics
            ),

            {IndexMs, IsExact} = emqx_retainer_index:condition(Index, Filter),
            Ms = [{{IndexMs, '_'}, [], ['$_']}],
            MatchedTopixByIndex0 = [
                emqx_retainer_index:restore_topic(Key)
             || {Key, _} <- ets:select(Tab, Ms)
            ],
            MatchedTopixByIndex =
                case IsExact of
                    true ->
                        MatchedTopixByIndex0;
                    false ->
                        lists:filter(
                            fun(Topic) ->
                                emqx_topic:match(Topic, Filter)
                            end,
                            MatchedTopixByIndex0
                        )
                end,

            lists:sort(MatchedTopicsDirectly) =:= lists:sort(MatchedTopixByIndex)
        end
    ).

index_t() ->
    ?LET(
        {Ints, Len},
        {non_empty(list(integer(1, ?MAX_TOPIC_LEN))), integer(1, ?MAX_INDEX_LEN)},
        lists:usort(lists:sublist(Ints, Len))
    ).

topic_t() ->
    ?LET(
        {Topic, Len},
        {non_empty(list(topic_segment_t())), integer(1, ?MAX_TOPIC_LEN)},
        lists:sublist(Topic, Len)
    ).

filter_t() ->
    ?LET(
        {TopicFilter, Len, MLWildcard},
        {
            non_empty(list(oneof([topic_segment_t(), '+']))),
            integer(1, ?MAX_FILTER_LEN),
            oneof([[], ['#']])
        },
        lists:sublist(TopicFilter, Len) ++ MLWildcard
    ).

topic_segment_t() ->
    ?LET(
        I,
        integer(0, ?CHARS - 1),
        <<($0 + I)>>
    ).
