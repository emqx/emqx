%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_replica_read).

-export([delivery_target/1, delivery_exists_cluster/1]).

-include("emqx_bcast.hrl").

%% bcast_msg is a ram_copies table. Its ETS backing store is a set keyed by
%% delivery_id, so this copies only the target integer instead of scanning or
%% copying the full delivery row with its device list.
delivery_target(Did) ->
    case ets:info(?TAB_MSG_REC) of
        undefined ->
            unavailable;
        _ ->
            try
                ets:lookup_element(
                    ?TAB_MSG_REC,
                    Did,
                    #bcast_msg.target_ack_count
                )
            of
                Target ->
                    {ok, Target}
            catch
                error:badarg ->
                    case delivery_exists_cluster(Did) of
                        exists -> unavailable;
                        gone -> gone;
                        unknown -> unavailable
                    end
            end
    end.

%% A local miss may be replication lag. Only conclude that the delivery is
%% gone when every running DB core confirms the key is absent.
delivery_exists_cluster(Did) ->
    Nodes = lists:usort(
        [node() | emqx_bcast:core_nodes() ++ mnesia:system_info(running_db_nodes)]
    ),
    try erpc:multicall(Nodes, ets, member, [?TAB_MSG_REC, Did], 1000) of
        Results when is_list(Results) ->
            case [Result || {ok, Result} <- Results] of
                Values when length(Values) =:= length(Results) ->
                    case lists:member(true, Values) of
                        true -> exists;
                        false -> gone
                    end;
                _ ->
                    unknown
            end
    catch
        _:_ -> unknown
    end.
