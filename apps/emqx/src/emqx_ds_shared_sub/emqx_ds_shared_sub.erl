%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub).

-export([
    declare/3,
    lookup/1,
    lookup/2,
    exists/2,
    destroy/1,
    destroy/2,
    list/2,
    strategy_module/1
]).

-export_type([
    options/0,
    strategy/0,
    info/0
]).

-include("emqx_mqtt.hrl").

-type id() :: emqx_ds_shared_sub_dl:id().

-type options() :: #{
    start_time => emqx_ds:time(),
    strategy => strategy()
}.

-type info() :: #{
    id := id(),
    created_at := integer(),
    group := emqx_types:group(),
    topic := emqx_types:topic(),
    start_time := integer()
}.

-type strategy() :: shard.

%%

-spec declare(emqx_types:group(), emqx_types:topic(), options()) -> {ok, info()} | emqx_ds:error(_).
declare(Group, Topic, Options = #{}) when
    is_binary(Group), is_binary(Topic)
->
    maybe
        Share = #share{group = Group, topic = Topic},
        {ok, _Pid} ?= emqx_ds_shared_sub_registry:get_leader_sync(Share, Options),
        case lookup(Group, Topic) of
            Info when is_map(Info) ->
                {ok, Info};
            undefined ->
                {error, recoverable, not_found}
        end
    end.

-spec lookup(emqx_types:group(), emqx_types:topic()) -> info() | undefined.
lookup(Group, Topic) ->
    lookup(emqx_ds_shared_sub_dl:mk_id(Group, Topic)).

-spec lookup(id()) -> info() | undefined.
lookup(Id) ->
    emqx_ds_shared_sub_dl:dirty_read_props(Id).

-spec exists(emqx_types:group(), emqx_types:topic()) -> boolean().
exists(Group, Topic) ->
    emqx_ds_shared_sub_dl:exists(emqx_ds_shared_sub_dl:mk_id(Group, Topic)).

-spec destroy(emqx_types:group(), emqx_types:topic()) -> ok | emqx_ds:error(_).
destroy(Group, Topic) ->
    destroy(emqx_ds_shared_sub_dl:mk_id(Group, Topic)).

-spec destroy(id()) -> boolean() | emqx_ds:error(_).
destroy(Id) ->
    maybe
        true ?= emqx_ds_shared_sub_dl:exists(Id),
        Share = emqx_ds_shared_sub_dl:decode_id(Id),
        %% Destruction is done via the leader:
        {ok, Leader} ?= emqx_ds_shared_sub_registry:get_leader_sync(Share, #{}),
        emqx_ds_shared_sub_leader:destroy(Leader)
    end.

-spec list(emqx_ds_shared_sub_dl:cursor() | undefined, pos_integer()) ->
    {[info()], emqx_ds_shared_sub_dl:cursor() | '$end_of_table'}.
list(undefined, Limit) ->
    list(emqx_ds_shared_sub_dl:make_iterator(), Limit);
list('$end_of_table' = EOT, _) ->
    {[], EOT};
list(Cursor, Limit) when is_binary(Cursor) ->
    emqx_ds_shared_sub_dl:iterator_next(Cursor, Limit).

%%

-spec strategy_module(strategy()) -> module().
strategy_module(shard) ->
    emqx_ds_shared_sub_strategy_shard.
