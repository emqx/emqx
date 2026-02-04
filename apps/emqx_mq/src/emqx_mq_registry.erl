%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_registry).

-moduledoc """
The module contains the registry of Message Queues.
""".

-include("emqx_mq_internal.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    create_tables/0,
    create/1,
    find/1,
    find_by_id/1,
    match/1,
    delete/1,
    update/2,
    list/0,
    list/2
]).

-dialyzer(no_improper_lists).

%% Only for testing/debugging.
-export([
    delete_all/0,
    create_pre_611_queue/1
]).

-define(MQ_REGISTRY_SHARD, emqx_mq_registry_shard).

-define(MQ_REGISTRY_INDEX_TAB, emqx_mq_registry_index).

-record(?MQ_REGISTRY_INDEX_TAB, {
    key :: emqx_topic_index:key(nil() | emqx_mq_types:mqid()) | '_',
    id :: emqx_mq_types:mqid() | '_',
    is_lastvalue :: boolean() | '_',
    key_expr :: emqx_variform:compiled() | undefined | '_',
    extra = #{} :: map() | '_'
}).

-type cursor() :: binary() | undefined.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec create_tables() -> [atom()].
create_tables() ->
    ok = mria:create_table(?MQ_REGISTRY_INDEX_TAB, [
        {type, ordered_set},
        {rlog_shard, ?MQ_REGISTRY_SHARD},
        {storage, disc_copies},
        {record_name, ?MQ_REGISTRY_INDEX_TAB},
        {attributes, record_info(fields, ?MQ_REGISTRY_INDEX_TAB)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]),
    [?MQ_REGISTRY_INDEX_TAB].

-doc """
Create a new MQ.
""".
%% Create a queue with `default' name, store it in pre-6.1.1 way.
-spec create(emqx_mq_types:mq()) ->
    {ok, emqx_mq_types:mq()}
    | {error, {queue_exists, emqx_mq_types:mq_handle()}}
    | {error, max_queue_count_reached}
    | {error, term()}.
%% Create a queue with a non-default name, store it in the new way.
create(#{is_lastvalue := IsLastValue} = MQ0) when
    (not IsLastValue) orelse (IsLastValue andalso is_map(map_get(key_expression, MQ0)))
->
    Id = emqx_guid:gen(),
    MQ = MQ0#{id => Id},
    Name = emqx_mq_prop:name(MQ),
    Key = make_key(MQ),
    IndexRecord = mq_to_record(MQ),
    maybe
        ok ?= validate_max_queue_count(),
        ok ?=
            do_create(
                [
                    #{
                        name => create_state,
                        create => fun() -> emqx_mq_state_storage:create_mq_state(MQ) end,
                        rollback => fun() -> drop_queue_state(MQ) end
                    },
                    #{
                        name => create_index,
                        create => fun() -> mria:dirty_write(IndexRecord) end,
                        rollback => fun() ->
                            mria:dirty_delete(?MQ_REGISTRY_INDEX_TAB, Key),
                            drop_consumer_state(MQ),
                            drop_queue_data(MQ)
                        end
                    },
                    #{
                        name => claim_name,
                        create => fun() -> emqx_mq_state_storage:set_name_index(Name, Id) end,
                        rollback => fun() -> emqx_mq_state_storage:destroy_name_index(Name, Id) end
                    }
                ],
                MQ,
                []
            ),
        {ok, MQ}
    end.

-doc """
Find all MQs matching the given concrete topic.
""".
%% NOTE
%% This function is called on any published message, so it should be very fast.
-spec match(emqx_types:topic()) -> [emqx_mq_types:mq_handle()].
match(Topic) ->
    Keys = emqx_topic_index:matches(Topic, ?MQ_REGISTRY_INDEX_TAB, []),
    lists:flatmap(
        fun(Key) ->
            case mnesia:dirty_read(?MQ_REGISTRY_INDEX_TAB, Key) of
                [] ->
                    [];
                [#?MQ_REGISTRY_INDEX_TAB{} = Rec] ->
                    [record_to_mq_handle(Rec)]
            end
        end,
        Keys
    ).

-doc """
Find the MQ by its name.
""".
-spec find(emqx_mq_types:mq_name()) -> {ok, emqx_mq_types:mq()} | not_found.
%% Name of legacy unnamed queues, lookup by topic index
find(?LEGACY_QUEUE_NAME(TopicFilter)) ->
    Key = make_default_queue_key(TopicFilter),
    case mnesia:dirty_read(?MQ_REGISTRY_INDEX_TAB, Key) of
        [] ->
            not_found;
        [#?MQ_REGISTRY_INDEX_TAB{id = Id}] ->
            emqx_mq_state_storage:find_mq(Id)
    end;
%% Normal name
find(Name) ->
    maybe
        {ok, Id} ?= emqx_mq_state_storage:find_id_by_name(Name),
        find_by_id(Id)
    end.

-doc """
Find the MQ by its ID.
""".
-spec find_by_id(emqx_mq_types:mqid()) -> {ok, emqx_mq_types:mq()} | not_found.
find_by_id(Id) ->
    emqx_mq_state_storage:find_mq(Id).

-doc """
Delete the MQ by its name.
""".
-spec delete(emqx_mq_types:mq_topic()) -> ok | not_found | {error, term()}.
%% Legacy previously unnamed queue, find through topic index
delete(?LEGACY_QUEUE_NAME(TopicFilter)) ->
    ?tp_debug(mq_registry_delete, #{topic_filter => TopicFilter}),
    Key = make_default_queue_key(TopicFilter),
    case mnesia:dirty_read(?MQ_REGISTRY_INDEX_TAB, Key) of
        [] ->
            not_found;
        [#?MQ_REGISTRY_INDEX_TAB{id = Id}] ->
            delete_by_id(Id)
    end;
%% Normal name, find through name index
delete(Name) ->
    ?tp_debug(mq_registry_delete, #{name => Name}),
    case emqx_mq_state_storage:find_id_by_name(Name) of
        {ok, Id} ->
            delete_by_id(Id);
        not_found ->
            not_found
    end.

-doc """
Delete the MQ by its Id.
""".
-spec delete_by_id(emqx_mq_types:mqid()) -> ok | not_found | {error, term()}.
delete_by_id(Id) ->
    maybe
        {ok, MQ} ?= emqx_mq_state_storage:find_mq(Id),
        Key = make_key(MQ),
        ok = mria:dirty_delete(?MQ_REGISTRY_INDEX_TAB, Key),
        ok ?= drop_consumer_state(MQ),
        ok ?= drop_queue_data(MQ),
        ok ?= drop_queue_state(MQ),
        Name = emqx_mq_prop:name(MQ),
        _ = emqx_mq_state_storage:destroy_name_index(Name, Id),
        ok
    end.

-doc """
Delete all MQs. Only for testing/maintenance.
""".
-spec delete_all() -> ok.
delete_all() ->
    _ = mria:clear_table(?MQ_REGISTRY_INDEX_TAB),
    _ = emqx_mq_state_storage:delete_all(),
    ok.

-doc """
Update the MQ by its topic filter.
* `is_lastvalue` flag cannot be updated.
* limited regular queues cannot be updated to unlimited regular queues and vice versa.
""".
-spec update(emqx_mq_types:mq_topic(), map()) ->
    {ok, emqx_mq_types:mq()}
    | not_found
    | {error,
        is_lastvalue_not_allowed_to_be_updated
        | limit_presence_cannot_be_updated_for_regular_queues
        | term()}.
update(Name, UpdateFields) ->
    maybe
        {ok, Id} ?= emqx_mq_state_storage:find_id_by_name(Name),
        update_by_id(Id, UpdateFields)
    end.

-doc """
Update the MQ by its ID.
""".
-spec update_by_id(emqx_mq_types:mqid(), map()) ->
    {ok, emqx_mq_types:mq()}
    | not_found
    | {error,
        is_lastvalue_not_allowed_to_be_updated
        | limit_presence_cannot_be_updated_for_regular_queues
        | term()}.
update_by_id(Id, UpdateFields0) ->
    UpdateFields = maps:without([topic_filter, id, name], UpdateFields0),
    maybe
        {ok, MQ} ?= find_by_id(Id),
        Key = make_key(MQ),
        IsLastvalueOld = emqx_mq_prop:is_lastvalue(MQ),
        IsLastvalueNew = emqx_mq_prop:is_lastvalue(UpdateFields),
        IsLimitedOld = emqx_mq_prop:is_limited(MQ),
        IsLimitedNew = emqx_mq_prop:is_limited(UpdateFields),
        NeedUpdateIndex = need_update_index(MQ, UpdateFields),
        case UpdateFields of
            _ when IsLastvalueOld =/= IsLastvalueNew ->
                {error, is_lastvalue_not_allowed_to_be_updated};
            _ when (not IsLastvalueNew) andalso (IsLimitedOld =/= IsLimitedNew) ->
                {error, limit_presence_cannot_be_updated_for_regular_queues};
            _ when NeedUpdateIndex ->
                case update_index(Key, Id, UpdateFields) of
                    ok ->
                        emqx_mq_state_storage:update_mq_state(Id, UpdateFields);
                    not_found ->
                        not_found
                end;
            _ ->
                emqx_mq_state_storage:update_mq_state(Id, UpdateFields)
        end
    end.

-doc """
List all MQs.
""".
-spec list() -> emqx_utils_stream:stream(emqx_mq_types:mq()).
list() ->
    emqx_utils_stream:map(
        fun({_Key, MQ}) ->
            MQ
        end,
        mq_record_stream_to_queues(mq_record_stream())
    ).

-doc """
List at most `Limit` MQs starting from `Cursor` position.
""".
-spec list(cursor(), non_neg_integer()) ->
    {ok, [emqx_mq_types:mq()], cursor()} | {error, bad_cursor}.
list(Cursor, Limit) when Limit >= 1 ->
    case key_from_cursor(Cursor) of
        {ok, Key} ->
            {MQs, CursorNext} = do_list(Key, Limit),
            {ok, MQs, CursorNext};
        {error, bad_cursor} ->
            {error, bad_cursor}
    end.

do_list(StartKey, Limit) ->
    KeyMQs0 = emqx_utils_stream:consume(
        emqx_utils_stream:limit_length(
            Limit + 1,
            mq_record_stream_to_queues(mq_record_stream(StartKey))
        )
    ),
    case length(KeyMQs0) < Limit + 1 of
        true ->
            {_Keys, MQs} = lists:unzip(KeyMQs0),
            {MQs, undefined};
        false ->
            KeyMQs = lists:sublist(KeyMQs0, Limit),
            {Keys, MQs} = lists:unzip(KeyMQs),
            Key = lists:last(Keys),
            NewCursor = cursor_from_key(Key),
            {MQs, NewCursor}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

drop_queue_data(MQ) ->
    safe_with_log(
        MQ,
        fun() -> emqx_mq_message_db:drop(MQ) end,
        mq_registry_drop_queue_data_error
    ).

drop_consumer_state(MQ) ->
    Id = emqx_mq_prop:id(MQ),
    case emqx_mq_consumer:find(Id) of
        {ok, ConsumerRef} ->
            emqx_mq_consumer:stop(ConsumerRef);
        not_found ->
            ok
    end,
    safe_with_log(
        MQ,
        fun() -> emqx_mq_state_storage:destroy_consumer_state(MQ) end,
        mq_registry_drop_consumer_error
    ).

drop_queue_state(MQ) ->
    safe_with_log(
        MQ,
        fun() -> emqx_mq_state_storage:destroy_mq_state(MQ) end,
        mq_registry_drop_queue_state_error
    ).

safe_with_log(MQ, Fun, ErrorLabel) ->
    try Fun() of
        ok ->
            ok;
        {error, Reason} ->
            ?tp(error, ErrorLabel, #{
                mq => MQ,
                reason => Reason
            }),
            {error, Reason}
    catch
        Class:Reason ->
            ?tp(error, ErrorLabel, #{
                mq => MQ,
                class => Class,
                reason => Reason
            }),
            {error, {Class, Reason}}
    end.

need_update_index(
    #{is_lastvalue := true, key_expression := OldKeyExpr} = _MQHandle,
    #{key_expression := NewKeyExpr} = _UpdateFields
) when NewKeyExpr =/= OldKeyExpr ->
    true;
need_update_index(#{limits := OldLimits} = _MQHandle, #{limits := NewLimits} = _UpdateFields) when
    OldLimits =/= NewLimits
->
    true;
need_update_index(_, _) ->
    false.

mq_record_stream() ->
    mq_record_stream(undefined).

mq_record_stream(Key) ->
    Stream = mq_ets_record_stream(Key),
    emqx_utils_stream:chainmap(
        fun(L) -> L end,
        Stream
    ).

key_from_cursor(undefined) ->
    {ok, undefined};
key_from_cursor(Cursor) ->
    try emqx_utils_json:decode(base64:decode(Cursor)) of
        #{<<"tf">> := TopicFilter, <<"id">> := Id} ->
            {ok, emqx_topic_index:make_key(TopicFilter, Id)};
        _ ->
            {error, bad_cursor}
    catch
        Class:Reason ->
            ?tp(warning, mq_registry_key_from_cursor_error, #{
                cursor => Cursor,
                class => Class,
                reason => Reason
            }),
            {error, bad_cursor}
    end.

cursor_from_key(Key) ->
    TopicFilter = emqx_topic_index:get_topic(Key),
    Id = emqx_topic_index:get_id(Key),
    base64:encode(emqx_utils_json:encode(#{<<"tf">> => TopicFilter, <<"id">> => Id})).

mq_ets_record_stream(Key) ->
    fun() ->
        case next_key(Key) of
            '$end_of_table' ->
                [];
            NextKey ->
                [ets:lookup(?MQ_REGISTRY_INDEX_TAB, NextKey) | mq_ets_record_stream(NextKey)]
        end
    end.

next_key(undefined) ->
    ets:first(?MQ_REGISTRY_INDEX_TAB);
next_key(Key) ->
    ets:next(?MQ_REGISTRY_INDEX_TAB, Key).

mq_record_stream_to_queues(Stream) ->
    emqx_utils_stream:chainmap(
        fun(#?MQ_REGISTRY_INDEX_TAB{key = Key, id = Id}) ->
            case emqx_mq_state_storage:find_mq(Id) of
                {ok, MQ} ->
                    [{Key, MQ}];
                not_found ->
                    []
            end
        end,
        Stream
    ).

make_key(MQ) ->
    make_key(MQ, emqx_mq_prop:name(MQ)).

make_key(#{topic_filter := TopicFilter}, ?LEGACY_QUEUE_NAME(_)) ->
    make_default_queue_key(TopicFilter);
make_key(#{topic_filter := TopicFilter, id := Id} = _MQ, _Name) ->
    emqx_topic_index:make_key(TopicFilter, Id).

make_default_queue_key(TopicFilter) ->
    emqx_topic_index:make_key(TopicFilter, []).

update_index(Key, Id, UpdateFields) ->
    {atomic, Result} = mria:transaction(?MQ_REGISTRY_SHARD, fun() ->
        case mnesia:read(?MQ_REGISTRY_INDEX_TAB, Key, write) of
            [] ->
                not_found;
            [#?MQ_REGISTRY_INDEX_TAB{id = Id, extra = Extra} = Rec] ->
                NewKeyExpr = maps:get(key_expression, UpdateFields, undefined),
                NewLimits = maps:get(limits, UpdateFields, ?DEFAULT_MQ_LIMITS),
                mnesia:write(Rec#?MQ_REGISTRY_INDEX_TAB{
                    key_expr = NewKeyExpr, extra = Extra#{limits => NewLimits}
                }),
                ok
        end
    end),
    Result.

record_to_mq_handle(#?MQ_REGISTRY_INDEX_TAB{
    key = Key, id = Id, key_expr = KeyExpr, is_lastvalue = IsLastValue, extra = Extra
}) ->
    #{
        id => Id,
        topic_filter => emqx_topic_index:get_topic(Key),
        is_lastvalue => IsLastValue,
        key_expression => KeyExpr,
        limits => maps:get(limits, Extra, ?DEFAULT_MQ_LIMITS)
    }.

mq_to_record(MQ) ->
    Key = make_key(MQ),
    mq_to_record(MQ, Key).

mq_to_record(#{id := Id, is_lastvalue := IsLastValue, limits := Limits} = MQ, Key) ->
    #?MQ_REGISTRY_INDEX_TAB{
        key = Key,
        id = Id,
        is_lastvalue = IsLastValue,
        key_expr = maps:get(key_expression, MQ, undefined),
        extra = #{
            limits => Limits
        }
    }.

queue_count() ->
    mnesia:table_info(?MQ_REGISTRY_INDEX_TAB, size).

validate_max_queue_count() ->
    case queue_count() >= emqx_mq_config:max_queue_count() of
        true ->
            {error, max_queue_count_reached};
        false ->
            ok
    end.

do_create([], _MQ, _Rollbacks) ->
    ok;
do_create([#{create := CreateFun, name := Label} = Step | Rest], MQ, Rollbacks) ->
    try CreateFun() of
        ok ->
            do_create(Rest, MQ, [Step | Rollbacks]);
        {error, Reason} ->
            ?tp(error, mq_registry_create_error, #{
                step => Label,
                mq => MQ,
                reason => Reason
            }),
            ok = do_rollback(MQ, Rollbacks),
            {error, Reason}
    catch
        Class:Reason ->
            ?tp(error, mq_registry_create_error, #{
                step => Label,
                mq => MQ,
                class => Class,
                reason => Reason
            }),
            ok = do_rollback(MQ, Rollbacks),
            {error, {Class, Reason}}
    end.

do_rollback(MQ, [#{rollback := RollbackFun, name := Label} = _Step | Rest]) ->
    try RollbackFun() of
        ok ->
            do_rollback(MQ, Rest);
        {error, Reason} ->
            ?tp(error, mq_registry_rollback_error, #{
                step => Label,
                mq => MQ,
                reason => Reason
            }),
            do_rollback(MQ, Rest)
    catch
        Class:Reason ->
            ?tp(error, mq_registry_rollback_error, #{
                step => Label,
                mq => MQ,
                class => Class,
                reason => Reason
            }),
            do_rollback(MQ, Rest)
    end;
do_rollback(_MQ, []) ->
    ok.

%%--------------------------------------------------------------------
%% Test helpers
%%--------------------------------------------------------------------

create_pre_611_queue(MQ0) ->
    MQ = maps:without([name], MQ0#{id => emqx_guid:gen()}),
    Key = make_default_queue_key(emqx_mq_prop:topic_filter(MQ)),
    Rec = mq_to_record(MQ, Key),
    {atomic, ok} =
        mria:transaction(?MQ_REGISTRY_SHARD, fun() ->
            mnesia:write(Rec)
        end),
    ok = emqx_mq_state_storage:create_mq_state(MQ).
