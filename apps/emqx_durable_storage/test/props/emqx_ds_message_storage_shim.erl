%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_message_storage_shim).

-export([open/0]).
-export([close/1]).
-export([store/5]).
-export([iterate/2]).

-type topic() :: list(binary()).
-type time() :: integer().

-opaque t() :: ets:tid().

-spec open() -> t().
open() ->
    ets:new(?MODULE, [ordered_set, {keypos, 1}]).

-spec close(t()) -> ok.
close(Tab) ->
    true = ets:delete(Tab),
    ok.

-spec store(t(), emqx_guid:guid(), time(), topic(), binary()) ->
    ok | {error, _TODO}.
store(Tab, MessageID, PublishedAt, Topic, Payload) ->
    true = ets:insert(Tab, {{PublishedAt, MessageID}, Topic, Payload}),
    ok.

-spec iterate(t(), emqx_ds:replay()) ->
    [binary()].
iterate(Tab, {TopicFilter, StartTime}) ->
    ets:foldr(
        fun({{PublishedAt, _}, Topic, Payload}, Acc) ->
            case emqx_topic:match(Topic, TopicFilter) of
                true when PublishedAt >= StartTime ->
                    [Payload | Acc];
                _ ->
                    Acc
            end
        end,
        [],
        Tab
    ).
