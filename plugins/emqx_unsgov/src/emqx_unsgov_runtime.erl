%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_unsgov_runtime).

-moduledoc """
In-memory runtime state for UNS models: compiled model cache (persistent_term),
topic index (ETS), filter-conflict tracking, and validation hot path.
""".

-export([
    build_runtime_state/1,
    runtime_apply_put_compiled/3,
    runtime_apply_delete/2,
    runtime_apply_activate/3,
    runtime_apply_deactivate/2,
    clear_all_compiled_models/0,
    clear_topic_index_table/0,
    ensure_topic_index_table/0,
    validate_topic/1,
    validate_message/3,
    validate_filter_conflict/4,
    check_filter_conflict/3
]).

%% Needed by bootstrap for boot_load_record
-export([
    get_compiled_model/1,
    put_compiled_model/2,
    add_model_filters_to_index/2,
    add_filter_owner/3
]).

-include("emqx_unsgov.hrl").
-define(TOPIC_INDEX_TAB, emqx_unsgov_topic_index).

%%--------------------------------------------------------------------
%% Runtime state build (on boot)
%%--------------------------------------------------------------------

build_runtime_state(State0) ->
    clear_all_compiled_models(),
    clear_topic_index_table(),
    Records0 = emqx_unsgov_store:list_model_records(),
    Records = lists:sort(
        fun(A, B) ->
            emqx_unsgov_store:model_id(A) =< emqx_unsgov_store:model_id(B)
        end,
        Records0
    ),
    FilterOwner =
        lists:foldl(
            fun(Record, Acc) -> boot_load_record(Record, Acc) end,
            maps:get(runtime_filter_owner, State0, #{}),
            Records
        ),
    State0#{runtime_filter_owner => FilterOwner}.

boot_load_record(Record, OwnerAcc) ->
    Id = emqx_unsgov_store:model_id(Record),
    case emqx_unsgov_model:compile(emqx_unsgov_store:model_raw(Record)) of
        {ok, Compiled} ->
            ok = put_compiled_model(Id, Compiled),
            IsActive = emqx_unsgov_store:model_active(Record),
            boot_maybe_activate(Id, Compiled, IsActive, OwnerAcc);
        {error, Reason} ->
            ?LOG(error, #{
                msg => "skip_model_on_boot_compile_failed",
                model_id => Id,
                reason => Reason
            }),
            OwnerAcc
    end.

boot_maybe_activate(_Id, _Compiled, false, OwnerAcc) ->
    OwnerAcc;
boot_maybe_activate(Id, Compiled, true, OwnerAcc) ->
    case check_filter_conflict(Compiled, Id, OwnerAcc) of
        ok ->
            add_model_filters_to_index(Id, Compiled),
            add_filter_owner(Id, Compiled, OwnerAcc);
        {error, #{cause := conflicting_topic_filter} = Reason} ->
            ?LOG(error, #{
                msg => "skip_active_model_on_boot_due_to_filter_conflict",
                model_id => Id,
                reason => Reason
            }),
            %% Reconcile persisted flag so DB matches runtime reality.
            ok = emqx_unsgov_store:set_model_active(Id, false),
            OwnerAcc
    end.

%%--------------------------------------------------------------------
%% Runtime apply operations
%%--------------------------------------------------------------------

runtime_apply_put_compiled(Compiled, Activate, State0) ->
    Id = maps:get(id, Compiled),
    OldCompiled = get_compiled_model(Id),
    ok = put_compiled_model(Id, Compiled),
    FilterOwner0 = maps:get(runtime_filter_owner, State0, #{}),
    WasActive = was_active_in_runtime(Id, OldCompiled, FilterOwner0),
    IsActive = Activate orelse WasActive,
    FilterOwner1 = maybe_remove_old_filters(Id, OldCompiled, WasActive, FilterOwner0),
    FilterOwner2 =
        case IsActive of
            true -> runtime_try_activate_filters(Id, Compiled, FilterOwner1);
            false -> FilterOwner1
        end,
    State0#{runtime_filter_owner => FilterOwner2}.

runtime_apply_delete(Id, State0) ->
    OldCompiled = get_compiled_model(Id),
    FilterOwner0 = maps:get(runtime_filter_owner, State0, #{}),
    case OldCompiled of
        undefined -> ok;
        _ -> delete_model_filters_from_index(Id, OldCompiled)
    end,
    erase_compiled_model(Id),
    FilterOwner =
        case OldCompiled of
            undefined -> FilterOwner0;
            _ -> remove_filter_owner(Id, OldCompiled, FilterOwner0)
        end,
    State0#{runtime_filter_owner => FilterOwner}.

runtime_apply_activate(_Id, Model, State0) ->
    case emqx_unsgov_model:compile(Model) of
        {ok, Compiled} ->
            runtime_apply_put_compiled(Compiled, true, State0);
        {error, _} ->
            State0
    end.

runtime_apply_deactivate(Id, State0) ->
    FilterOwner0 = maps:get(runtime_filter_owner, State0, #{}),
    Compiled0 = get_compiled_model(Id),
    case Compiled0 of
        undefined -> ok;
        Compiled1 -> delete_model_filters_from_index(Id, Compiled1)
    end,
    FilterOwner =
        case Compiled0 of
            undefined -> FilterOwner0;
            Compiled2 -> remove_filter_owner(Id, Compiled2, FilterOwner0)
        end,
    State0#{runtime_filter_owner => FilterOwner}.

maybe_remove_old_filters(Id, OldCompiled, true, FilterOwner) when OldCompiled =/= undefined ->
    delete_model_filters_from_index(Id, OldCompiled),
    remove_filter_owner(Id, OldCompiled, FilterOwner);
maybe_remove_old_filters(_Id, _OldCompiled, _WasActive, FilterOwner) ->
    FilterOwner.

runtime_try_activate_filters(Id, Compiled, FilterOwner) ->
    case check_filter_conflict(Compiled, Id, FilterOwner) of
        ok ->
            add_model_filters_to_index(Id, Compiled),
            add_filter_owner(Id, Compiled, FilterOwner);
        {error, #{cause := conflicting_topic_filter} = Reason} ->
            ?LOG(error, #{
                msg => "skip_runtime_apply_due_to_filter_conflict",
                model_id => Id,
                reason => Reason
            }),
            FilterOwner
    end.

%% Check if a model was active by seeing if it owns any filters.
was_active_in_runtime(_Id, undefined, _FilterOwner) ->
    false;
was_active_in_runtime(Id, Compiled, FilterOwner) ->
    Filters = maps:get(topic_filters, Compiled, []),
    lists:any(
        fun(FilterWords) ->
            Filter = filter_words_to_topic(FilterWords),
            maps:get(Filter, FilterOwner, undefined) =:= Id
        end,
        Filters
    ).

%%--------------------------------------------------------------------
%% Compiled model persistent_term cache
%%--------------------------------------------------------------------

compiled_model_pt_key(Id) ->
    {emqx_unsgov_store, compiled_model, Id}.

compiled_model_ids_pt_key() ->
    {emqx_unsgov_store, compiled_model_ids}.

get_compiled_model(Id) ->
    persistent_term:get(compiled_model_pt_key(Id), undefined).

put_compiled_model(Id, Compiled) ->
    persistent_term:put(compiled_model_pt_key(Id), Compiled),
    Ids0 = persistent_term:get(compiled_model_ids_pt_key(), []),
    persistent_term:put(compiled_model_ids_pt_key(), lists:usort([Id | Ids0])),
    ok.

erase_compiled_model(Id) ->
    persistent_term:erase(compiled_model_pt_key(Id)),
    Ids0 = persistent_term:get(compiled_model_ids_pt_key(), []),
    persistent_term:put(
        compiled_model_ids_pt_key(),
        [X || X <- Ids0, X =/= Id]
    ),
    ok.

clear_all_compiled_models() ->
    Ids = persistent_term:get(compiled_model_ids_pt_key(), []),
    lists:foreach(fun(Id) -> persistent_term:erase(compiled_model_pt_key(Id)) end, Ids),
    persistent_term:erase(compiled_model_ids_pt_key()),
    ok.

%%--------------------------------------------------------------------
%% Topic index ETS
%%--------------------------------------------------------------------

add_model_filters_to_index(Id, Compiled) ->
    Filters = maps:get(topic_filters, Compiled, []),
    lists:foreach(
        fun(FilterWords) ->
            emqx_topic_index:insert(filter_words_to_topic(FilterWords), Id, Id, ?TOPIC_INDEX_TAB)
        end,
        Filters
    ),
    ok.

delete_model_filters_from_index(Id, Compiled) ->
    Filters = maps:get(topic_filters, Compiled, []),
    lists:foreach(
        fun(FilterWords) ->
            emqx_topic_index:delete(filter_words_to_topic(FilterWords), Id, ?TOPIC_INDEX_TAB)
        end,
        Filters
    ),
    ok.

filter_words_to_topic(FilterWords) ->
    iolist_to_binary(lists:join(<<"/">>, FilterWords)).

topic_index_matches(Topic) ->
    try emqx_topic_index:matches(Topic, ?TOPIC_INDEX_TAB, [unique]) of
        Matches -> Matches
    catch
        error:badarg -> []
    end.

ensure_topic_index_table() ->
    case ets:info(?TOPIC_INDEX_TAB) of
        undefined ->
            _ = ets:new(?TOPIC_INDEX_TAB, [
                named_table,
                ordered_set,
                protected,
                {read_concurrency, true}
            ]),
            ok;
        _ ->
            ok
    end.

clear_topic_index_table() ->
    case ets:info(?TOPIC_INDEX_TAB) of
        undefined ->
            ok;
        _ ->
            true = ets:delete_all_objects(?TOPIC_INDEX_TAB),
            ok
    end.

%%--------------------------------------------------------------------
%% Filter conflict checking
%%--------------------------------------------------------------------

validate_filter_conflict(_Compiled, _Id, false, _State) ->
    ok;
validate_filter_conflict(Compiled, Id, true, State) ->
    check_filter_conflict(Compiled, Id, maps:get(runtime_filter_owner, State, #{})).

check_filter_conflict(Compiled, SelfId, FilterOwner) ->
    Filters = maps:get(topic_filters, Compiled, []),
    check_filter_conflict_loop(Filters, SelfId, FilterOwner).

check_filter_conflict_loop([], _SelfId, _FilterOwner) ->
    ok;
check_filter_conflict_loop([FilterWords | Rest], SelfId, FilterOwner) ->
    Filter = filter_words_to_topic(FilterWords),
    case maps:get(Filter, FilterOwner, undefined) of
        undefined ->
            check_filter_conflict_loop(Rest, SelfId, FilterOwner);
        SelfId ->
            check_filter_conflict_loop(Rest, SelfId, FilterOwner);
        ExistingId ->
            {error, #{
                cause => conflicting_topic_filter,
                screening_filter => Filter,
                conflict_with => ExistingId
            }}
    end.

add_filter_owner(Id, Compiled, FilterOwner0) ->
    Filters = maps:get(topic_filters, Compiled, []),
    lists:foldl(
        fun(FilterWords, Acc) ->
            Acc#{filter_words_to_topic(FilterWords) => Id}
        end,
        FilterOwner0,
        Filters
    ).

remove_filter_owner(Id, Compiled, FilterOwner0) ->
    Filters = maps:get(topic_filters, Compiled, []),
    lists:foldl(
        fun(FilterWords, Acc) ->
            Filter = filter_words_to_topic(FilterWords),
            case maps:get(Filter, Acc, undefined) of
                Id -> maps:remove(Filter, Acc);
                _ -> Acc
            end
        end,
        FilterOwner0,
        Filters
    ).

%%--------------------------------------------------------------------
%% Validation hot path
%%--------------------------------------------------------------------

validate_topic(Topic) ->
    do_validate_topic(Topic).

validate_message(ModelId, Topic, Payload) ->
    do_validate_message_for_model(ModelId, Topic, Payload).

do_validate_topic(Topic) ->
    with_selected_model(
        Topic,
        fun(Selected) ->
            do_validate_topic_selected(Selected, Topic)
        end
    ).

do_validate_topic_selected({Id, Compiled}, Topic) ->
    case emqx_unsgov_model:validate_topic(Compiled, Topic) of
        allow -> {allow, Id};
        {deny, Reason} -> {deny, Id, Reason}
    end.

do_validate_message_for_model(ModelId, Topic, Payload) ->
    ValidatePayload = emqx_unsgov_config:validate_payload(),
    case get_compiled_model(ModelId) of
        undefined ->
            %% Race condition, the model is deleted
            %% after looked up when validating the topic
            {deny, undefined, topic_nomatch};
        Compiled ->
            do_validate_message_selected({ModelId, Compiled}, Topic, Payload, ValidatePayload)
    end.

with_selected_model(Topic, OnSelected) ->
    case select_screened_model(Topic) of
        undefined -> {deny, undefined, topic_nomatch};
        Selected -> OnSelected(Selected)
    end.

do_validate_message_selected({Id, Compiled}, Topic, Payload, ValidatePayload) ->
    case emqx_unsgov_model:validate_message(Compiled, Topic, Payload, ValidatePayload) of
        allow -> {allow, Id};
        {deny, Reason} -> {deny, Id, Reason}
    end.

select_screened_model(Topic) ->
    Matches = topic_index_matches(Topic),
    Ids0 = [emqx_topic_index:get_id(M) || M <- Matches],
    Ids = lists:usort([Id || Id <- Ids0, is_binary(Id)]),
    find_first_compiled(Ids).

find_first_compiled([]) ->
    undefined;
find_first_compiled([Id | Rest]) ->
    case get_compiled_model(Id) of
        undefined -> find_first_compiled(Rest);
        Compiled -> {Id, Compiled}
    end.
