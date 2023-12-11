%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_message_storage_bitmask_shim).

-include_lib("emqx/include/emqx.hrl").

-export([open/0]).
-export([close/1]).
-export([store/2]).
-export([iterate/2]).

-opaque t() :: ets:tid().

-export_type([t/0]).

-spec open() -> t().
open() ->
    ets:new(?MODULE, [ordered_set, {keypos, 1}]).

-spec close(t()) -> ok.
close(Tab) ->
    true = ets:delete(Tab),
    ok.

-spec store(t(), emqx_types:message()) ->
    ok | {error, _TODO}.
store(Tab, Msg = #message{id = MessageID, timestamp = PublishedAt}) ->
    true = ets:insert(Tab, {{PublishedAt, MessageID}, Msg}),
    ok.

-spec iterate(t(), emqx_ds:replay()) ->
    [binary()].
iterate(Tab, {TopicFilter0, StartTime}) ->
    TopicFilter = iolist_to_binary(lists:join("/", TopicFilter0)),
    ets:foldr(
        fun({{PublishedAt, _}, Msg = #message{topic = Topic}}, Acc) ->
            case emqx_topic:match(Topic, TopicFilter) of
                true when PublishedAt >= StartTime ->
                    [Msg | Acc];
                _ ->
                    Acc
            end
        end,
        [],
        Tab
    ).
