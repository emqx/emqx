%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_skill_registry).

-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([
    lookup/2,
    reconcile/0,
    statuses/0,
    register_type/2,
    unregister_type/1,
    resolve_type/1
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(TAB, ?MODULE).
-define(TYPE_TAB, emqx_agent_skill_type_registry).
-define(STATUS_TAB, emqx_agent_skill_status_registry).

-type skill_id() :: binary().
-type skill_type() :: binary().

-type skill() :: #{
    skill_id := skill_id(),
    type := skill_type(),
    module := module(),
    display_name := binary(),
    description := binary(),
    context => term(),
    input_schema => map()
}.

-export_type([skill/0, skill_id/0, skill_type/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec lookup(skill_type(), skill_id()) -> {ok, skill()} | {error, not_found}.
lookup(Type, SkillId) ->
    case ets:lookup(?TAB, {Type, SkillId}) of
        [{_Key, Skill}] -> {ok, Skill};
        [] -> {error, not_found}
    end.

-spec reconcile() -> ok.
reconcile() ->
    gen_server:call(?MODULE, reconcile, infinity).

-spec statuses() -> map().
statuses() ->
    maps:from_list([
        {status_key(Type, SkillId), status_to_external(Status)}
     || {{Type, SkillId}, Status} <- ets:tab2list(?STATUS_TAB)
    ]).

-spec register_type(skill_type(), module()) -> ok.
register_type(Type, Module) when is_binary(Type), is_atom(Module) ->
    gen_server:call(?MODULE, {register_type, Type, Module}).

-spec unregister_type(skill_type()) -> ok.
unregister_type(Type) when is_binary(Type) ->
    gen_server:call(?MODULE, {unregister_type, Type}).

-spec resolve_type(skill_type()) -> module().
resolve_type(Type) when is_binary(Type) ->
    case ets:lookup(?TYPE_TAB, Type) of
        [{Type, Module}] -> Module;
        [] -> throw(unknown_type)
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    _ = ets:new(?TAB, [named_table, set, public, {read_concurrency, true}]),
    _ = ets:new(?TYPE_TAB, [named_table, set, public, {read_concurrency, true}]),
    _ = ets:new(?STATUS_TAB, [named_table, set, public, {read_concurrency, true}]),
    {ok, #{}}.

handle_call(reconcile, _From, State) ->
    {reply, do_reconcile(), State};
handle_call({register_type, Type, Module}, _From, State) ->
    true = ets:insert(?TYPE_TAB, {Type, Module}),
    {reply, ok, State};
handle_call({unregister_type, Type}, _From, State) ->
    true = ets:delete(?TYPE_TAB, Type),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Internal operations
%%--------------------------------------------------------------------

insert_runtime(#{skill_id := SkillId, type := Type} = Skill) ->
    true = ets:insert(?TAB, {{Type, SkillId}, Skill}),
    put_status(Type, SkillId, running, null),
    ok.

drop_runtime(Type, SkillId) ->
    OldSkill = ets:lookup(?TAB, {Type, SkillId}),
    true = ets:delete(?TAB, {Type, SkillId}),
    true = ets:delete(?STATUS_TAB, {Type, SkillId}),
    destroy_runtime(OldSkill),
    ok.

do_reconcile() ->
    Desired = desired_skills(),
    DesiredKeys = maps:keys(Desired),
    lists:foreach(fun(Key) -> maybe_drop_removed(Key, DesiredKeys) end, status_keys()),
    lists:foreach(fun(Key) -> maybe_drop_removed(Key, DesiredKeys) end, runtime_keys()),
    maps:foreach(fun reconcile_skill/2, Desired),
    ok.

desired_skills() ->
    maps:from_list([
        {{maps:get(<<"type">>, Skill), maps:get(<<"id">>, Skill)}, Skill}
     || Skill <- emqx_agent_config:parsed_config([skills], [])
    ]).

status_keys() ->
    [Key || {Key, _Status} <- ets:tab2list(?STATUS_TAB)].

runtime_keys() ->
    [Key || {Key, _Skill} <- ets:tab2list(?TAB)].

maybe_drop_removed({Type, SkillId}, DesiredKeys) ->
    case lists:member({Type, SkillId}, DesiredKeys) of
        true -> ok;
        false -> drop_runtime(Type, SkillId)
    end.

reconcile_skill({Type, SkillId}, SkillConfig) ->
    ok = drop_runtime(Type, SkillId),
    case resolve_type_safe(Type) of
        {ok, Module} ->
            Ctx = create_context(SkillConfig),
            case Module:create(Ctx) of
                {ok, Skill} ->
                    insert_runtime(Skill);
                {error, Reason} ->
                    put_status(Type, SkillId, failed, Reason)
            end;
        {error, Reason} ->
            put_status(Type, SkillId, missing_type, Reason)
    end.

resolve_type_safe(Type) ->
    try resolve_type(Type) of
        Module -> {ok, Module}
    catch
        throw:Reason -> {error, Reason}
    end.

create_context(#{<<"id">> := SkillId} = SkillConfig) ->
    Runtime = maps:from_list([runtime_field(K, V) || {K, V} <- maps:to_list(SkillConfig)]),
    maps:put(skill_id, SkillId, maps:remove(id, Runtime)).

runtime_field(<<"type">>, V) -> {type, V};
runtime_field(<<"id">>, V) -> {id, V};
runtime_field(<<"desc">>, V) -> {desc, V};
runtime_field(<<"topic_prefix">>, V) -> {topic_prefix, V};
runtime_field(<<"payload_schema">>, <<>>) -> {payload_schema, undefined};
runtime_field(<<"payload_schema">>, V) -> {payload_schema, decode_schema(V)};
runtime_field(<<"request_payload_schema">>, <<>>) -> {request_payload_schema, undefined};
runtime_field(<<"request_payload_schema">>, V) -> {request_payload_schema, decode_schema(V)};
runtime_field(<<"method">>, V) -> {method, V};
runtime_field(<<"url">>, V) -> {url, V};
runtime_field(<<"headers">>, V) -> {headers, V};
runtime_field(<<"input_schema">>, V) -> {input_schema, decode_schema(V)};
runtime_field(<<"query">>, V) -> {query, V};
runtime_field(<<"resource">>, V) -> {resource, V};
runtime_field(K, V) -> {K, V}.

decode_schema(V) when is_binary(V) ->
    emqx_utils_json:decode(V);
decode_schema(V) ->
    V.

destroy_runtime([]) ->
    ok;
destroy_runtime([{_Key, #{module := Module} = Skill}]) ->
    _ = catch Module:destroy(Skill),
    ok.

put_status(Type, SkillId, Status, Error) ->
    true = ets:insert(
        ?STATUS_TAB,
        {{Type, SkillId}, #{
            type => Type,
            skill_id => SkillId,
            status => Status,
            error => Error
        }}
    ),
    ok.

status_key(Type, SkillId) ->
    <<Type/binary, "@", SkillId/binary>>.

status_to_external(#{status := Status, error := Error}) ->
    #{
        <<"status">> => status_to_binary(Status),
        <<"error">> => error_to_json(Error)
    }.

status_to_binary(Status) when is_atom(Status) ->
    atom_to_binary(Status, utf8);
status_to_binary(Status) when is_binary(Status) ->
    Status.

error_to_json(null) ->
    null;
error_to_json(undefined) ->
    null;
error_to_json(Error) when is_binary(Error) ->
    Error;
error_to_json(Error) ->
    iolist_to_binary(io_lib:format("~0p", [Error])).
