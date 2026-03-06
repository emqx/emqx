%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_unsgov_store).

-moduledoc """
Persists UNS models in mria tables. Gen_server serializes writes and
delegates runtime state to `emqx_unsgov_runtime` and bootstrap loading
to `emqx_unsgov_bootstrap`.
""".

-behaviour(gen_server).

-export([
    create_tables/0,
    start_link/0,
    list_models/0,
    get_model/1,
    put_model/2,
    delete_model/1,
    activate/1,
    deactivate/1,
    validate_topic/1,
    validate_message/3
]).

%% Test/debug only — not used in production code.
-export([
    reset/0
]).

%% Model management cluster_rpc calls.
-export([
    apply_put_local/3,
    apply_delete_local/2,
    apply_activate_local/3,
    apply_deactivate_local/2
]).

%% Helpers exported for sibling modules.
-export([
    list_model_records/0,
    get_model_record/1,
    get_model_record_or_not_found/1,
    set_model_active/2,
    model_id/1,
    model_raw/1,
    model_active/1,
    model_summary/1,
    model_updated_at_ms/1,
    format_entry/1,
    to_bin/1
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include("emqx_unsgov.hrl").

create_tables() ->
    ok = mria:create_table(?MODEL_TAB, [
        {type, ordered_set},
        {rlog_shard, ?DB_SHARD},
        {storage, disc_copies},
        {attributes, record_info(fields, ?MODEL_TAB)}
    ]),
    ok = mria:wait_for_tables([?MODEL_TAB]),
    ok.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Reset all models and runtime state. Test/debug only.
reset() ->
    gen_server:call(?MODULE, reset).

list_models() ->
    gen_server:call(?MODULE, list_models).

get_model(Id) ->
    gen_server:call(?MODULE, {get_model, to_bin(Id)}).

put_model(Model, Activate) ->
    gen_server:call(?MODULE, {put_model, Model, Activate}).

delete_model(Id) ->
    gen_server:call(?MODULE, {delete_model, to_bin(Id)}).

activate(Id) ->
    gen_server:call(?MODULE, {activate, to_bin(Id)}).

deactivate(Id) ->
    gen_server:call(?MODULE, {deactivate, to_bin(Id)}).

validate_topic(Topic) ->
    emqx_unsgov_runtime:validate_topic(Topic).

validate_message(ModelId, Topic, Payload) ->
    emqx_unsgov_runtime:validate_message(ModelId, Topic, Payload).

apply_put_local(_Model, _Activate, #{kind := initiate}) ->
    ok;
apply_put_local(Model, Activate, _ClusterOpts) ->
    gen_server:call(?MODULE, {runtime_apply_put, Model, Activate}).

apply_delete_local(_Id, #{kind := initiate}) ->
    ok;
apply_delete_local(Id, _ClusterOpts) ->
    gen_server:call(?MODULE, {runtime_apply_delete, to_bin(Id)}).

apply_activate_local(_Id, _Model, #{kind := initiate}) ->
    ok;
apply_activate_local(Id, Model, _ClusterOpts) ->
    gen_server:call(?MODULE, {runtime_apply_activate, to_bin(Id), Model}).

apply_deactivate_local(_Id, #{kind := initiate}) ->
    ok;
apply_deactivate_local(Id, _ClusterOpts) ->
    gen_server:call(?MODULE, {runtime_apply_deactivate, to_bin(Id)}).

init([]) ->
    ok = create_tables(),
    emqx_unsgov_runtime:ensure_topic_index_table(),
    ok = emqx_unsgov_bootstrap:load(),
    {ok, emqx_unsgov_runtime:build_runtime_state(#{runtime_filter_owner => #{}})}.

handle_call(reset, _From, _State) ->
    _ = mria:clear_table(?MODEL_TAB),
    emqx_unsgov_runtime:clear_all_compiled_models(),
    emqx_unsgov_runtime:clear_topic_index_table(),
    {reply, ok, #{runtime_filter_owner => #{}}};
handle_call(list_models, _From, State) ->
    Entries = [format_entry(Record) || Record <- list_model_records()],
    Sorted = lists:sort(
        fun(#{id := A}, #{id := B}) -> A =< B end,
        Entries
    ),
    {reply, {ok, Sorted}, State};
handle_call({get_model, Id}, _From, State) ->
    case get_model_record(Id) of
        {ok, Record} ->
            {reply, {ok, format_entry(Record)}, State};
        error ->
            {reply, {error, not_found}, State}
    end;
handle_call({put_model, Model, Activate}, _From, State) ->
    maybe
        {ok, Compiled} ?= emqx_unsgov_model:compile(Model),
        Id = maps:get(id, Compiled),
        WillBeActive = Activate orelse is_model_active(Id),
        ok ?= emqx_unsgov_runtime:validate_filter_conflict(Compiled, Id, WillBeActive, State),
        do_put_model(Compiled, Model, Activate, State)
    else
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call({activate, Id}, _From, State) ->
    maybe
        {ok, Record} ?= get_model_record_or_not_found(Id),
        Model = model_raw(Record),
        {ok, Compiled} ?= emqx_unsgov_model:compile(Model),
        ok ?=
            emqx_unsgov_runtime:check_filter_conflict(
                Compiled, Id, maps:get(runtime_filter_owner, State, #{})
            ),
        ok = set_model_active(Id, true),
        State1 = emqx_unsgov_runtime:runtime_apply_activate(Id, Model, State),
        _ = multicall_apply_activate(Id, Model),
        {reply, ok, State1}
    else
        {error, _} = Error ->
            {reply, Error, State}
    end;
handle_call({delete_model, Id}, _From, State0) ->
    do_delete_model(Id, State0);
handle_call({deactivate, Id}, _From, State) ->
    case get_model_record(Id) of
        {ok, _} ->
            ok = set_model_active(Id, false),
            State1 = emqx_unsgov_runtime:runtime_apply_deactivate(Id, State),
            _ = multicall_apply_deactivate(Id),
            {reply, ok, State1};
        error ->
            {reply, {error, not_found}, State}
    end;
handle_call({runtime_apply_put, Model, Activate}, _From, State0) ->
    State =
        case emqx_unsgov_model:compile(Model) of
            {ok, Compiled} ->
                emqx_unsgov_runtime:runtime_apply_put_compiled(Compiled, Activate, State0);
            {error, _} ->
                State0
        end,
    {reply, ok, State};
handle_call({runtime_apply_delete, Id}, _From, State0) ->
    {reply, ok, emqx_unsgov_runtime:runtime_apply_delete(Id, State0)};
handle_call({runtime_apply_activate, Id, Model}, _From, State0) ->
    {reply, ok, emqx_unsgov_runtime:runtime_apply_activate(Id, Model, State0)};
handle_call({runtime_apply_deactivate, Id}, _From, State0) ->
    {reply, ok, emqx_unsgov_runtime:runtime_apply_deactivate(Id, State0)};
handle_call(_Call, _From, State) ->
    {reply, {error, bad_request}, State}.

do_put_model(Compiled, Model, Activate, State) ->
    Id = maps:get(id, Compiled),
    Ts = erlang:system_time(millisecond),
    Record = #?MODEL_TAB{
        id = Id,
        model = maps:get(raw, Compiled),
        summary = emqx_unsgov_model:summary(Compiled),
        active = Activate orelse is_model_active(Id),
        updated_at_ms = Ts
    },
    ok = mria:dirty_write(?MODEL_TAB, Record),
    State1 = emqx_unsgov_runtime:runtime_apply_put_compiled(Compiled, Activate, State),
    _ = multicall_apply_put(Model, Activate),
    {reply, {ok, format_entry(Record)}, State1}.

do_delete_model(Id, State0) ->
    case get_model_record(Id) of
        {ok, _Record} ->
            ok = mria:dirty_delete(?MODEL_TAB, Id),
            ok = emqx_unsgov_metrics:delete_model(Id),
            State1 = emqx_unsgov_runtime:runtime_apply_delete(Id, State0),
            _ = multicall_apply_delete(Id),
            {reply, ok, State1};
        error ->
            {reply, {error, not_found}, State0}
    end.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Record accessors & helpers
%%--------------------------------------------------------------------

format_entry(Record) ->
    #{
        id => model_id(Record),
        active => model_active(Record),
        updated_at_ms => model_updated_at_ms(Record),
        summary => model_summary(Record),
        model => model_raw(Record)
    }.

is_model_active(Id) ->
    case get_model_record(Id) of
        {ok, Record} -> model_active(Record);
        error -> false
    end.

set_model_active(Id, Active) ->
    case ets:lookup(?MODEL_TAB, Id) of
        [Record] ->
            mria:dirty_write(?MODEL_TAB, Record#?MODEL_TAB{active = Active});
        _ ->
            ok
    end.

list_model_records() ->
    ets:tab2list(?MODEL_TAB).

get_model_record(Id) ->
    case ets:lookup(?MODEL_TAB, Id) of
        [Record] -> {ok, Record};
        _ -> error
    end.

get_model_record_or_not_found(Id) ->
    case get_model_record(Id) of
        {ok, Record} -> {ok, Record};
        error -> {error, not_found}
    end.

model_id(#?MODEL_TAB{id = Id}) ->
    Id.

model_raw(#?MODEL_TAB{model = Model}) ->
    Model.

model_active(#?MODEL_TAB{active = Active}) ->
    Active.

model_summary(#?MODEL_TAB{summary = Summary}) ->
    Summary.

model_updated_at_ms(#?MODEL_TAB{updated_at_ms = Ts}) ->
    Ts.

to_bin(V) when is_binary(V) -> V;
to_bin(V) when is_atom(V) -> atom_to_binary(V, utf8);
to_bin(V) when is_list(V) -> unicode:characters_to_binary(V).

%%--------------------------------------------------------------------
%% Cluster RPC multicall
%%--------------------------------------------------------------------

multicall_apply_put(Model, Activate) ->
    multicall_apply(apply_put_local, [Model, Activate]).

multicall_apply_delete(Id) ->
    multicall_apply(apply_delete_local, [Id]).

multicall_apply_activate(Id, Model) ->
    multicall_apply(apply_activate_local, [Id, Model]).

multicall_apply_deactivate(Id) ->
    multicall_apply(apply_deactivate_local, [Id]).

multicall_apply(F, A) ->
    try emqx_cluster_rpc:multicall(?MODULE, F, A, all, 5000) of
        ok ->
            ok;
        Other ->
            ?LOG(warning, #{
                msg => "cluster_rpc_unexpected_result",
                function => F,
                result => Other
            }),
            ok
    catch
        Class:Reason ->
            ?LOG(warning, #{
                msg => "cluster_rpc_apply_failed",
                function => F,
                kind => Class,
                reason => Reason
            }),
            ok
    end.
