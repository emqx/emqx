%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_subscription).

-include("emqx_bcast.hrl").

-export([init/0]).
-export([add/3, remove/3, clear/2, match/2, backfill/3, replace/3, topics/1]).

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

add(ClientId, Pid, Filter) ->
    case ets:lookup(?TAB, ClientId) of
        [#bcast_subscription{topics = Topics}] ->
            ets:insert(?TAB, #bcast_subscription{
                clientid = ClientId, pid = Pid, topics = [Filter | Topics]
            });
        [] ->
            ets:insert(?TAB, #bcast_subscription{
                clientid = ClientId, pid = Pid, topics = [Filter]
            })
    end.

remove(ClientId, Pid, {TopicFilter, _Qos}) ->
    case ets:lookup(?TAB, ClientId) of
        [#bcast_subscription{pid = Pid, topics = Topics}] ->
            case lists:keydelete(TopicFilter, 1, Topics) of
                [] ->
                    ets:delete(?TAB, ClientId);
                NewTopics ->
                    ets:insert(?TAB, #bcast_subscription{
                        clientid = ClientId, pid = Pid, topics = NewTopics
                    })
            end;
        [#bcast_subscription{}] ->
            %% stale unsubscribe from an old connection after takeover
            ok;
        [] ->
            ok
    end.

clear(ClientId, Pid) ->
    case ets:lookup(?TAB, ClientId) of
        [#bcast_subscription{pid = Pid}] ->
            ets:delete(?TAB, ClientId);
        _ ->
            %% stale disconnect from an old connection after takeover
            ok
    end.

match(ClientId, Topic) ->
    case ets:lookup(?TAB, ClientId) of
        [#bcast_subscription{topics = Topics}] ->
            Matched = lists:filtermap(
                fun({Filter, Qos}) ->
                    case emqx_topic:match(Topic, Filter) of
                        true -> {true, Qos};
                        false -> false
                    end
                end,
                Topics
            ),
            case Matched of
                [] -> false;
                Qoses -> {ok, lists:max(Qoses)}
            end;
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

replace(ClientId, Pid, Topics) ->
    case Topics of
        [] ->
            clear(ClientId, Pid);
        _ ->
            ets:insert(?TAB, #bcast_subscription{
                clientid = ClientId, pid = Pid, topics = Topics
            })
    end.

topics(ClientId) ->
    case ets:lookup(?TAB, ClientId) of
        [#bcast_subscription{topics = Topics}] -> Topics;
        [] -> []
    end.
