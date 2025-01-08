%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_validation_registry).

-behaviour(gen_server).

%% API
-export([
    lookup/1,
    insert/2,
    update/3,
    delete/2,
    reindex_positions/2,

    matching_validations/1,

    %% metrics
    get_metrics/1,
    inc_matched/1,
    inc_succeeded/1,
    inc_failed/1,

    start_link/0,
    metrics_worker_spec/0
]).

%% `gen_server' API
-export([
    init/1,
    handle_call/3,
    handle_cast/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(VALIDATION_TOPIC_INDEX, emqx_schema_validation_index).
-define(VALIDATION_TAB, emqx_schema_validation_tab).

-define(METRIC_NAME, schema_validation).
-define(METRICS, [
    'matched',
    'succeeded',
    'failed'
]).
-define(RATE_METRICS, ['matched']).

-type validation_name() :: binary().
-type validation() :: _TODO.
-type position_index() :: pos_integer().

-record(reindex_positions, {new_validations :: [validation()], old_validations :: [validation()]}).
-record(insert, {pos :: position_index(), validation :: validation()}).
-record(update, {old :: validation(), pos :: position_index(), new :: validation()}).
-record(delete, {validation :: validation(), pos :: position_index()}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec lookup(validation_name()) ->
    {ok, validation()} | {error, not_found}.
lookup(Name) ->
    case emqx_utils_ets:lookup_value(?VALIDATION_TAB, Name, undefined) of
        undefined ->
            {error, not_found};
        Validation ->
            {ok, Validation}
    end.

-spec reindex_positions([validation()], [validation()]) -> ok.
reindex_positions(NewValidations, OldValidations) ->
    gen_server:call(
        ?MODULE,
        #reindex_positions{
            new_validations = NewValidations,
            old_validations = OldValidations
        },
        infinity
    ).

-spec insert(position_index(), validation()) -> ok.
insert(Pos, Validation) ->
    gen_server:call(?MODULE, #insert{pos = Pos, validation = Validation}, infinity).

-spec update(validation(), position_index(), validation()) -> ok.
update(Old, Pos, New) ->
    gen_server:call(?MODULE, #update{old = Old, pos = Pos, new = New}, infinity).

-spec delete(validation(), position_index()) -> ok.
delete(Validation, Pos) ->
    gen_server:call(?MODULE, #delete{validation = Validation, pos = Pos}, infinity).

%% @doc Returns a list of matching validation names, sorted by their configuration order.
-spec matching_validations(emqx_types:topic()) -> [validation()].
matching_validations(Topic) ->
    Validations0 =
        lists:flatmap(
            fun(M) ->
                case emqx_topic_index:get_record(M, ?VALIDATION_TOPIC_INDEX) of
                    [Name] ->
                        [Name];
                    _ ->
                        []
                end
            end,
            emqx_topic_index:matches(Topic, ?VALIDATION_TOPIC_INDEX, [unique])
        ),
    lists:flatmap(
        fun(Name) ->
            case lookup(Name) of
                {ok, Validation} ->
                    [Validation];
                _ ->
                    []
            end
        end,
        Validations0
    ).

-spec metrics_worker_spec() -> supervisor:child_spec().
metrics_worker_spec() ->
    emqx_metrics_worker:child_spec(schema_validation_metrics, ?METRIC_NAME).

-spec get_metrics(validation_name()) -> emqx_metrics_worker:metrics().
get_metrics(Name) ->
    emqx_metrics_worker:get_metrics(?METRIC_NAME, Name).

-spec inc_matched(validation_name()) -> ok.
inc_matched(Name) ->
    emqx_metrics_worker:inc(?METRIC_NAME, Name, 'matched').

-spec inc_succeeded(validation_name()) -> ok.
inc_succeeded(Name) ->
    emqx_metrics_worker:inc(?METRIC_NAME, Name, 'succeeded').

-spec inc_failed(validation_name()) -> ok.
inc_failed(Name) ->
    emqx_metrics_worker:inc(?METRIC_NAME, Name, 'failed').

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(_) ->
    create_tables(),
    State = #{},
    {ok, State}.

handle_call(
    #reindex_positions{
        new_validations = NewValidations,
        old_validations = OldValidations
    },
    _From,
    State
) ->
    do_reindex_positions(NewValidations, OldValidations),
    {reply, ok, State};
handle_call(#insert{pos = Pos, validation = Validation}, _From, State) ->
    do_insert(Pos, Validation),
    {reply, ok, State};
handle_call(#update{old = OldValidation, pos = Pos, new = NewValidation}, _From, State) ->
    ok = do_update(OldValidation, Pos, NewValidation),
    {reply, ok, State};
handle_call(#delete{validation = Validation, pos = Pos}, _From, State) ->
    do_delete(Validation, Pos),
    {reply, ok, State};
handle_call(_Call, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

create_tables() ->
    _ = emqx_utils_ets:new(?VALIDATION_TOPIC_INDEX, [public, ordered_set, {read_concurrency, true}]),
    _ = emqx_utils_ets:new(?VALIDATION_TAB, [public, ordered_set, {read_concurrency, true}]),
    ok.

do_reindex_positions(NewValidations, OldValidations) ->
    lists:foreach(
        fun({Pos, Validation}) ->
            #{topics := Topics} = Validation,
            delete_topic_index(Pos, Topics)
        end,
        lists:enumerate(OldValidations)
    ),
    lists:foreach(
        fun({Pos, Validation}) ->
            #{
                name := Name,
                topics := Topics
            } = Validation,
            do_insert_into_tab(Name, Validation, Pos),
            update_topic_index(Name, Pos, Topics)
        end,
        lists:enumerate(NewValidations)
    ).

do_insert(Pos, Validation) ->
    #{
        enable := Enabled,
        name := Name,
        topics := Topics
    } = Validation,
    maybe_create_metrics(Name),
    do_insert_into_tab(Name, Validation, Pos),
    Enabled andalso update_topic_index(Name, Pos, Topics),
    ok.

do_update(OldValidation, Pos, NewValidation) ->
    #{topics := OldTopics} = OldValidation,
    #{
        enable := Enabled,
        name := Name,
        topics := NewTopics
    } = NewValidation,
    maybe_create_metrics(Name),
    do_insert_into_tab(Name, NewValidation, Pos),
    delete_topic_index(Pos, OldTopics),
    Enabled andalso update_topic_index(Name, Pos, NewTopics),
    ok.

do_delete(Validation, Pos) ->
    #{
        name := Name,
        topics := Topics
    } = Validation,
    ets:delete(?VALIDATION_TAB, Name),
    delete_topic_index(Pos, Topics),
    drop_metrics(Name),
    ok.

do_insert_into_tab(Name, Validation0, Pos) ->
    Validation = transform_validation(Validation0#{pos => Pos}),
    ets:insert(?VALIDATION_TAB, {Name, Validation}),
    ok.

maybe_create_metrics(Name) ->
    case emqx_metrics_worker:has_metrics(?METRIC_NAME, Name) of
        true ->
            ok = emqx_metrics_worker:reset_metrics(?METRIC_NAME, Name);
        false ->
            ok = emqx_metrics_worker:create_metrics(?METRIC_NAME, Name, ?METRICS, ?RATE_METRICS)
    end.

drop_metrics(Name) ->
    ok = emqx_metrics_worker:clear_metrics(?METRIC_NAME, Name).

update_topic_index(Name, Pos, Topics) ->
    lists:foreach(
        fun(Topic) ->
            true = emqx_topic_index:insert(Topic, Pos, Name, ?VALIDATION_TOPIC_INDEX)
        end,
        Topics
    ).

delete_topic_index(Pos, Topics) ->
    lists:foreach(
        fun(Topic) ->
            true = emqx_topic_index:delete(Topic, Pos, ?VALIDATION_TOPIC_INDEX)
        end,
        Topics
    ).

transform_validation(Validation = #{checks := Checks}) ->
    Validation#{checks := lists:map(fun transform_check/1, Checks)}.

transform_check(#{type := sql, sql := SQL}) ->
    {ok, Check} = emqx_schema_validation:parse_sql_check(SQL),
    Check;
transform_check(Check) ->
    Check.
