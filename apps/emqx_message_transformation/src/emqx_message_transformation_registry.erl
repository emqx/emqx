%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_message_transformation_registry).

-behaviour(gen_server).

%% API
-export([
    lookup/1,
    insert/2,
    update/3,
    delete/2,
    reindex_positions/2,

    matching_transformations/1,

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

-define(TRANSFORMATION_TOPIC_INDEX, emqx_message_transformation_index).
-define(TRANSFORMATION_TAB, emqx_message_transformation_tab).

-define(METRIC_NAME, message_transformation).
-define(METRICS, [
    'matched',
    'succeeded',
    'failed'
]).
-define(RATE_METRICS, ['matched']).

-type transformation_name() :: binary().
%% TODO
-type transformation() :: #{atom() => term()}.
-type position_index() :: pos_integer().

-record(reindex_positions, {
    new_transformations :: [transformation()],
    old_transformations :: [transformation()]
}).
-record(insert, {pos :: position_index(), transformation :: transformation()}).
-record(update, {old :: transformation(), pos :: position_index(), new :: transformation()}).
-record(delete, {transformation :: transformation(), pos :: position_index()}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec lookup(transformation_name()) ->
    {ok, transformation()} | {error, not_found}.
lookup(Name) ->
    case emqx_utils_ets:lookup_value(?TRANSFORMATION_TAB, Name, undefined) of
        undefined ->
            {error, not_found};
        Transformation ->
            {ok, Transformation}
    end.

-spec reindex_positions([transformation()], [transformation()]) -> ok.
reindex_positions(NewTransformations, OldTransformations) ->
    gen_server:call(
        ?MODULE,
        #reindex_positions{
            new_transformations = NewTransformations,
            old_transformations = OldTransformations
        },
        infinity
    ).

-spec insert(position_index(), transformation()) -> ok.
insert(Pos, Transformation) ->
    gen_server:call(?MODULE, #insert{pos = Pos, transformation = Transformation}, infinity).

-spec update(transformation(), position_index(), transformation()) -> ok.
update(Old, Pos, New) ->
    gen_server:call(?MODULE, #update{old = Old, pos = Pos, new = New}, infinity).

-spec delete(transformation(), position_index()) -> ok.
delete(Transformation, Pos) ->
    gen_server:call(?MODULE, #delete{transformation = Transformation, pos = Pos}, infinity).

%% @doc Returns a list of matching transformation names, sorted by their configuration order.
-spec matching_transformations(emqx_types:topic()) -> [transformation()].
matching_transformations(Topic) ->
    Transformations0 =
        lists:flatmap(
            fun(M) ->
                case emqx_topic_index:get_record(M, ?TRANSFORMATION_TOPIC_INDEX) of
                    [Name] ->
                        [Name];
                    _ ->
                        []
                end
            end,
            emqx_topic_index:matches(Topic, ?TRANSFORMATION_TOPIC_INDEX, [unique])
        ),
    lists:flatmap(
        fun(Name) ->
            case lookup(Name) of
                {ok, Transformation} ->
                    [Transformation];
                _ ->
                    []
            end
        end,
        Transformations0
    ).

-spec metrics_worker_spec() -> supervisor:child_spec().
metrics_worker_spec() ->
    emqx_metrics_worker:child_spec(message_transformation_metrics, ?METRIC_NAME).

-spec get_metrics(transformation_name()) -> emqx_metrics_worker:metrics().
get_metrics(Name) ->
    emqx_metrics_worker:get_metrics(?METRIC_NAME, Name).

-spec inc_matched(transformation_name()) -> ok.
inc_matched(Name) ->
    emqx_metrics_worker:inc(?METRIC_NAME, Name, 'matched').

-spec inc_succeeded(transformation_name()) -> ok.
inc_succeeded(Name) ->
    emqx_metrics_worker:inc(?METRIC_NAME, Name, 'succeeded').

-spec inc_failed(transformation_name()) -> ok.
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
        new_transformations = NewTransformations,
        old_transformations = OldTransformations
    },
    _From,
    State
) ->
    do_reindex_positions(NewTransformations, OldTransformations),
    {reply, ok, State};
handle_call(#insert{pos = Pos, transformation = Transformation}, _From, State) ->
    do_insert(Pos, Transformation),
    {reply, ok, State};
handle_call(#update{old = OldTransformation, pos = Pos, new = NewTransformation}, _From, State) ->
    ok = do_update(OldTransformation, Pos, NewTransformation),
    {reply, ok, State};
handle_call(#delete{transformation = Transformation, pos = Pos}, _From, State) ->
    do_delete(Transformation, Pos),
    {reply, ok, State};
handle_call(_Call, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

create_tables() ->
    _ = emqx_utils_ets:new(?TRANSFORMATION_TOPIC_INDEX, [
        public, ordered_set, {read_concurrency, true}
    ]),
    _ = emqx_utils_ets:new(?TRANSFORMATION_TAB, [public, ordered_set, {read_concurrency, true}]),
    ok.

do_reindex_positions(NewTransformations, OldTransformations) ->
    lists:foreach(
        fun({Pos, Transformation}) ->
            #{topics := Topics} = Transformation,
            delete_topic_index(Pos, Topics)
        end,
        lists:enumerate(OldTransformations)
    ),
    lists:foreach(
        fun({Pos, Transformation}) ->
            #{
                name := Name,
                topics := Topics
            } = Transformation,
            do_insert_into_tab(Name, Transformation, Pos),
            upsert_topic_index(Name, Pos, Topics)
        end,
        lists:enumerate(NewTransformations)
    ).

do_insert(Pos, Transformation) ->
    #{
        enable := Enabled,
        name := Name,
        topics := Topics
    } = Transformation,
    maybe_create_metrics(Name),
    do_insert_into_tab(Name, Transformation, Pos),
    Enabled andalso upsert_topic_index(Name, Pos, Topics),
    ok.

do_update(OldTransformation, Pos, NewTransformation) ->
    #{topics := OldTopics} = OldTransformation,
    #{
        enable := Enabled,
        name := Name,
        topics := NewTopics
    } = NewTransformation,
    maybe_create_metrics(Name),
    do_insert_into_tab(Name, NewTransformation, Pos),
    delete_topic_index(Pos, OldTopics),
    Enabled andalso upsert_topic_index(Name, Pos, NewTopics),
    ok.

do_delete(Transformation, Pos) ->
    #{
        name := Name,
        topics := Topics
    } = Transformation,
    ets:delete(?TRANSFORMATION_TAB, Name),
    delete_topic_index(Pos, Topics),
    drop_metrics(Name),
    ok.

do_insert_into_tab(Name, Transformation0, Pos) ->
    Transformation = Transformation0#{pos => Pos},
    ets:insert(?TRANSFORMATION_TAB, {Name, Transformation}),
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

upsert_topic_index(Name, Pos, Topics) ->
    lists:foreach(
        fun(Topic) ->
            true = emqx_topic_index:insert(Topic, Pos, Name, ?TRANSFORMATION_TOPIC_INDEX)
        end,
        Topics
    ).

delete_topic_index(Pos, Topics) ->
    lists:foreach(
        fun(Topic) ->
            true = emqx_topic_index:delete(Topic, Pos, ?TRANSFORMATION_TOPIC_INDEX)
        end,
        Topics
    ).
