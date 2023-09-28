%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kinesis_consumer).

-include_lib("stdlib/include/ms_transform.hrl").

-define(BRIDGE_SHARD, emqx_bridge_shard).
-define(KINESIS_TAB, emqx_bridge_kinesis_consumer_tab).
-define(ASSIGNMENT_KEY(InstanceId, Node), {?MODULE, InstanceId, assignments, Node}).
-define(LSN_KEY(InstanceId, ShardId), {?MODULE, InstanceId, lsn, ShardId}).
-record(kinesis_consumer, {
    key :: term(),
    value :: term()
}).

-type instance_id() :: binary().
-type shard_id() :: binary().

-export([
    create_tables/0,

    all_nodes/0,

    list_shard_ids/2,

    get_shard_issues/2,
    get_all_shard_assignments/1,
    get_shard_assignments/2,
    set_shard_assignments/2,
    set_shard_assignments/3,
    merge_shard_assignments/2,
    delete_shard_assignments/2,

    get_shard_lsn/2,
    set_shard_lsn/3
]).

%% internal exports for app
-export([transaction/1]).

-export_type([shard_id/0]).

create_tables() ->
    ok = mria:create_table(?KINESIS_TAB, [
        {type, ordered_set},
        {rlog_shard, ?BRIDGE_SHARD},
        %% FIXME: check type
        {storage, rocksdb_copies},
        {record_name, kinesis_consumer},
        {attributes, record_info(fields, kinesis_consumer)}
    ]),
    ok = mria:wait_for_tables([?KINESIS_TAB]),
    ok.

-spec get_all_shard_assignments(instance_id()) -> #{node() => [shard_id()]}.
get_all_shard_assignments(InstanceId) ->
    MS = ets:fun2ms(
        fun(#kinesis_consumer{key = ?ASSIGNMENT_KEY(Id, N), value = V}) when
            Id =:= InstanceId
        ->
            {N, V}
        end
    ),
    maps:from_list(mnesia:select(?KINESIS_TAB, MS, read)).

get_shard_assignments(InstanceId, Node) ->
    Key = ?ASSIGNMENT_KEY(InstanceId, Node),
    read(Key, _IfAbsent = []).

set_shard_assignments(InstanceId, NodeAssignments) when is_map(NodeAssignments) ->
    maps:foreach(
        fun(Node, ShardIds) when is_list(ShardIds) ->
            set_shard_assignments(InstanceId, Node, ShardIds)
        end,
        NodeAssignments
    ).

set_shard_assignments(InstanceId, Node, Assignments) when is_list(Assignments) ->
    Key = ?ASSIGNMENT_KEY(InstanceId, Node),
    write(Key, lists:usort(Assignments)).

merge_shard_assignments(InstanceId, NodeAssignments) when is_map(NodeAssignments) ->
    maps:foreach(
        fun(Node, NewAssignments) ->
            OldAssignments = get_shard_assignments(InstanceId, Node),
            Assignments = OldAssignments ++ NewAssignments,
            set_shard_assignments(InstanceId, Node, Assignments)
        end,
        NodeAssignments
    ).

delete_shard_assignments(InstanceId, Node) ->
    Key = ?ASSIGNMENT_KEY(InstanceId, Node),
    delete(Key).

get_shard_lsn(InstanceId, ShardId) ->
    Key = ?LSN_KEY(InstanceId, ShardId),
    read(Key, _IfAbsent = undefined).

set_shard_lsn(InstanceId, ShardId, LSN) ->
    Key = ?LSN_KEY(InstanceId, ShardId),
    write(Key, LSN).

%%--------------------------------------------------------------------
%% Internal helper fns
%%--------------------------------------------------------------------

read(Key, IfAbsent) ->
    case mnesia:read(?KINESIS_TAB, Key, read) of
        [] ->
            IfAbsent;
        [#kinesis_consumer{value = Value}] ->
            Value
    end.

write(Key, Value) ->
    Record = #kinesis_consumer{key = Key, value = Value},
    ok = mnesia:write(?KINESIS_TAB, Record, write).

delete(Key) ->
    ok = mnesia:delete(?KINESIS_TAB, Key, write).

transaction(Fun) ->
    mria:transaction(?BRIDGE_SHARD, Fun).

-spec all_nodes() -> [node()].
all_nodes() ->
    emqx:cluster_nodes(all).

all_up_nodes() ->
    emqx:cluster_nodes(running).

list_shard_ids(InstanceId, StreamName) ->
    ecpool:pick_and_do(
        InstanceId,
        {emqx_bridge_kinesis_connector_client, list_shard_ids, [StreamName]},
        no_handover
    ).

get_shard_issues(ShardIds, InstanceId) ->
    Nodes = all_nodes(),
    %% todo: ekka API for this?
    DownNodes = Nodes -- all_up_nodes(),
    %% #{node() => [shard_id()]}
    {atomic, NodeAssignments} = transaction(fun() -> get_all_shard_assignments(InstanceId) end),
    AssignmentToNodes =
        maps:fold(
            fun(Node, As, Acc) ->
                SIdsToNode = maps:from_keys(As, [Node]),
                maps:merge_with(fun(_ShardId, Ns1, Ns2) -> Ns1 ++ Ns2 end, SIdsToNode, Acc)
            end,
            #{},
            NodeAssignments
        ),
    NodesWithoutAssignments = Nodes -- maps:keys(NodeAssignments),
    DuplicateAssignments = maps:filter(fun(_, Ns) -> length(Ns) > 1 end, AssignmentToNodes),
    AssignedShardIds = lists:concat(maps:values(NodeAssignments)),
    Unassigned = ShardIds -- AssignedShardIds,
    UnexpectedShards =
        maps:fold(
            fun(Node, SIds, Acc) ->
                Unexpected = SIds -- ShardIds,
                case Unexpected of
                    [] ->
                        Acc;
                    [_ | _] ->
                        Acc#{Node => Unexpected}
                end
            end,
            #{},
            NodeAssignments
        ),
    WithDownNodes =
        maps:from_list([
            {SId, N}
         || {N, SIds} <- maps:to_list(maps:with(DownNodes, NodeAssignments)),
            SId <- SIds
        ]),
    WithUnknownNodes =
        maps:from_list([
            {SId, N}
         || {N, SIds} <- maps:to_list(maps:without(Nodes, NodeAssignments)),
            SId <- SIds
        ]),
    %% shards assigned to down nodes
    #{
        all_nodes => Nodes,
        down_nodes => DownNodes,
        all_shard_ids => ShardIds,
        with_down_nodes => WithDownNodes,
        %% shards not yet assigned
        unassigned => Unassigned,
        %% shards assigned to nodes outside cluster (died and never came back?)
        with_unknown_nodes => WithUnknownNodes,
        %% shard ids assigned to more than one node
        duplicate_assignments => DuplicateAssignments,
        %% shards not in the input shard list.  possibly shards that disappeared after
        %% merge operations / resharding.  returns a mapping `#{node() => [shard_id()]}'
        %% for easier removal.
        unexpected_shards => UnexpectedShards,
        %% all nodes in the cluster should at least have an empty list of shards assigned
        %% to them.
        nodes_without_assignments => NodesWithoutAssignments
    }.
