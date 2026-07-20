%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_subscription).

-include("emqx_bcast.hrl").

-export([init/0]).
-export([add/3, remove/3, clear/2, match/2, backfill/3]).

-define(TAB, bcast_subscription).

init() ->
    case ets:info(?TAB) of
        undefined ->
            ets:new(?TAB, [
                named_table,
                public,
                set,
                {keypos, #bcast_subscription.clientid},
                {read_concurrency, true},
                {write_concurrency, true}
            ]);
        _ ->
            ok
    end.

add(_ClientId, _Pid, Filter) ->
    case ets:lookup(?TAB, _ClientId) of
        [#bcast_subscription{topics = Topics}] ->
            ets:insert(?TAB, #bcast_subscription{
                clientid = _ClientId, pid = _Pid, topics = [Filter | Topics]
            });
        [] ->
            ets:insert(?TAB, #bcast_subscription{
                clientid = _ClientId, pid = _Pid, topics = [Filter]
            })
    end.

remove(ClientId, _Pid, {TopicFilter, _Qos}) ->
    case ets:lookup(?TAB, ClientId) of
        [#bcast_subscription{topics = Topics}] ->
            case lists:keydelete(TopicFilter, 1, Topics) of
                [] ->
                    ets:delete(?TAB, ClientId);
                NewTopics ->
                    ets:insert(?TAB, #bcast_subscription{
                        clientid = ClientId, pid = undefined, topics = NewTopics
                    })
            end;
        [] ->
            ok
    end.

clear(ClientId, _Pid) ->
    ets:delete(?TAB, ClientId).

match(ClientId, Topic) ->
    case ets:lookup(?TAB, ClientId) of
        [#bcast_subscription{topics = Topics}] ->
            lists:any(fun({Filter, _Qos}) -> emqx_topic:match(Topic, Filter) end, Topics);
        [] ->
            false
    end.

backfill(ClientId, Pid, Subscriptions) ->
    Topics = maps:fold(
        fun(Filter, Opts, Acc) -> [{Filter, maps:get(qos, Opts, 0)} | Acc] end,
        [],
        Subscriptions
    ),
    case Topics of
        [] ->
            ok;
        _ ->
            ets:insert(?TAB, #bcast_subscription{
                clientid = ClientId, pid = Pid, topics = Topics
            })
    end.
