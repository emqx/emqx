%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_config).

-moduledoc """
Owns the plugin configuration boundary for emqx_agent.

Public CRUD functions work with the raw plugin config shape: binary-keyed maps
as received from the plugin API and as rendered back to the UI. They perform a
read-update-write cycle over the whole plugin config: fetch the raw config with
emqx_plugins:get_config/2, update only the relevant part, and persist it with
emqx_mgmt_api_plugins:put_plugin_config/2. Avro validation is performed by the
plugin subsystem when persisting the config.

put_plugin_config/2 is the propagation point. It distributes the raw config
update through the plugin config machinery and eventually calls the plugin's
on_config_changed callback on each node. That callback calls update_config/2,
which normalizes the propagated Avro JSON config for runtime use and stores the
normalized binary-keyed config in persistent_term.

Runtime code should read parsed_config/0,1,2 instead of re-reading raw plugin
config. API code should use the raw CRUD functions so responses preserve the
same external shape that was submitted by users.
""".

-export([
    init_config/0,
    update_config/2,
    config_schema/0,
    clear_config_schema/0,
    parsed_config/0,
    parsed_config/1,
    parsed_config/2
]).

-export([
    create_skill/1,
    list_skills/0,
    lookup_skill/2,
    update_skill/3,
    delete_skill/2
]).

-export([
    create_connection/1,
    list_connections/0,
    lookup_connection/1,
    update_connection/2,
    delete_connection/1
]).

-export([
    create_pipeline/1,
    list_pipelines/0,
    lookup_pipeline/1,
    update_pipeline/2,
    delete_pipeline/1
]).

-define(CONFIG_KEY, {?MODULE, parsed_config}).
-define(CONFIG_SCHEMA_KEY, {?MODULE, config_schema}).
-define(SKILLS, <<"skills">>).
-define(SKILL_TYPE, <<"type">>).
-define(SKILL_ID, <<"id">>).
-define(CONNECTIONS, <<"connections">>).
-define(CONNECTION_ID, <<"id">>).
-define(PIPELINES, <<"pipelines">>).
-define(PIPELINE_ID, <<"pipeline_id">>).

-type skill_type() :: binary().
-type skill_id() :: binary().
-type connection_id() :: binary().
-type raw_config() :: map().
-type raw_skill() :: map().
-type raw_connection() :: map().
-type pipeline_id() :: binary().
-type raw_pipeline() :: map().

%%--------------------------------------------------------------------
%% Config lifecycle
%%--------------------------------------------------------------------

-spec init_config() -> ok | {error, term()}.
init_config() ->
    case parse_config(current_config()) of
        {ok, Parsed} ->
            cache_config(Parsed);
        {error, _} = Error ->
            Error
    end.

-spec update_config(raw_config(), raw_config()) -> ok | {error, term()}.
update_config(_OldConfig, NewConfig) ->
    case parse_config(NewConfig) of
        {ok, Parsed} ->
            cache_config(Parsed);
        {error, _} = Error ->
            Error
    end.

-spec parsed_config() -> map().
parsed_config() ->
    persistent_term:get(?CONFIG_KEY, #{}).

-spec parsed_config([term()]) -> term().
parsed_config(Path) ->
    parsed_config(Path, undefined).

-spec parsed_config([term()], term()) -> term().
parsed_config(Path, Default) ->
    emqx_utils_maps:deep_get([path_key(Key) || Key <- Path], parsed_config(), Default).

-spec config_schema() -> map().
config_schema() ->
    case persistent_term:get(?CONFIG_SCHEMA_KEY, undefined) of
        undefined ->
            Schema = read_config_schema(),
            persistent_term:put(?CONFIG_SCHEMA_KEY, Schema),
            Schema;
        Schema ->
            Schema
    end.

-spec clear_config_schema() -> ok.
clear_config_schema() ->
    _ = persistent_term:erase(?CONFIG_SCHEMA_KEY),
    ok.

%%--------------------------------------------------------------------
%% Skill CRUD
%%--------------------------------------------------------------------

-spec create_skill(raw_skill()) -> ok | {error, term()}.
create_skill(Body) when is_map(Body) ->
    case required_skill_key(Body) of
        {ok, Type, SkillId} ->
            case wrap_skill(Body) of
                {ok, Skill} ->
                    update_raw_config(fun(Config0) ->
                        Skills0 = entries(Config0, ?SKILLS),
                        Pred = skill_pred(Type, SkillId),
                        case find_entry(Pred, Skills0) of
                            {ok, _} ->
                                {error, already_exists};
                            {error, not_found} ->
                                {ok, Config0#{?SKILLS => Skills0 ++ [Skill]}}
                        end
                    end);
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end;
create_skill(_) ->
    {error, invalid_skill}.

-spec list_skills() -> [raw_skill()].
list_skills() ->
    normalize_entries(entries(current_config(), ?SKILLS)).

-spec lookup_skill(skill_type(), skill_id()) -> {ok, raw_skill()} | {error, not_found}.
lookup_skill(Type, SkillId) ->
    find_entry(skill_pred(Type, SkillId), list_skills()).

-spec update_skill(skill_type(), skill_id(), raw_skill()) ->
    {ok, raw_skill()} | {error, term()}.
update_skill(Type, SkillId, Body) when is_map(Body) ->
    case wrap_skill(put_entry_fields(Body, [{?SKILL_TYPE, Type}, {?SKILL_ID, SkillId}])) of
        {ok, Skill} ->
            case
                update_raw_config(fun(Config0) ->
                    Skills0 = entries(Config0, ?SKILLS),
                    case replace_entry(skill_pred(Type, SkillId), Skill, Skills0) of
                        {ok, Skills} -> {ok, Config0#{?SKILLS => Skills}};
                        {error, _} = Error -> Error
                    end
                end)
            of
                ok -> lookup_skill(Type, SkillId);
                {error, _} = Error -> Error
            end;
        {error, _} = Error ->
            Error
    end;
update_skill(_, _, _) ->
    {error, invalid_skill}.

-spec delete_skill(skill_type(), skill_id()) -> ok | {error, term()}.
delete_skill(Type, SkillId) ->
    update_raw_config(fun(Config0) ->
        Skills0 = entries(Config0, ?SKILLS),
        case remove_entry(skill_pred(Type, SkillId), Skills0) of
            {ok, Skills} -> {ok, Config0#{?SKILLS => Skills}};
            {error, _} = Error -> Error
        end
    end).

%%--------------------------------------------------------------------
%% Connection CRUD
%%--------------------------------------------------------------------

-spec create_connection(raw_connection()) -> ok | {error, term()}.
create_connection(Body) when is_map(Body) ->
    case required_id(Body) of
        {ok, ConnectionId} ->
            Conn = wrap_connection(Body),
            update_raw_config(fun(Config0) ->
                Connections0 = entries(Config0, ?CONNECTIONS),
                case find_entry(id_pred(ConnectionId), Connections0) of
                    {ok, _} ->
                        {error, already_exists};
                    {error, not_found} ->
                        {ok, Config0#{?CONNECTIONS => Connections0 ++ [Conn]}}
                end
            end);
        {error, _} = Error ->
            Error
    end;
create_connection(_) ->
    {error, invalid_connection}.

-spec list_connections() -> [raw_connection()].
list_connections() ->
    normalize_entries(entries(current_config(), ?CONNECTIONS)).

-spec lookup_connection(connection_id()) -> {ok, raw_connection()} | {error, not_found}.
lookup_connection(ConnectionId) ->
    find_entry(id_pred(ConnectionId), list_connections()).

-spec update_connection(connection_id(), raw_connection()) ->
    {ok, raw_connection()} | {error, term()}.
update_connection(ConnectionId, Body) when is_map(Body) ->
    Conn = wrap_connection(put_entry_fields(Body, [{?CONNECTION_ID, ConnectionId}])),
    case
        update_raw_config(fun(Config0) ->
            Connections0 = entries(Config0, ?CONNECTIONS),
            case replace_entry(id_pred(ConnectionId), Conn, Connections0) of
                {ok, Connections} -> {ok, Config0#{?CONNECTIONS => Connections}};
                {error, _} = Error -> Error
            end
        end)
    of
        ok -> lookup_connection(ConnectionId);
        {error, _} = Error -> Error
    end;
update_connection(_, _) ->
    {error, invalid_connection}.

-spec delete_connection(connection_id()) -> ok | {error, term()}.
delete_connection(ConnectionId) ->
    update_raw_config(fun(Config0) ->
        Connections0 = entries(Config0, ?CONNECTIONS),
        case remove_entry(id_pred(ConnectionId), Connections0) of
            {ok, Connections} -> {ok, Config0#{?CONNECTIONS => Connections}};
            {error, _} = Error -> Error
        end
    end).

%%--------------------------------------------------------------------
%% Pipeline CRUD
%%--------------------------------------------------------------------

-spec create_pipeline(raw_pipeline()) -> ok | {error, term()}.
create_pipeline(Body) when is_map(Body) ->
    case required_pipeline_id(Body) of
        {ok, PipelineId} ->
            case wrap_pipeline(Body) of
                {ok, Pipeline} ->
                    update_raw_config(fun(Config0) ->
                        Pipelines0 = entries(Config0, ?PIPELINES),
                        case replace_entry(pipeline_pred(PipelineId), Pipeline, Pipelines0) of
                            {ok, Pipelines} ->
                                {ok, Config0#{?PIPELINES => Pipelines}};
                            {error, not_found} ->
                                {ok, Config0#{?PIPELINES => Pipelines0 ++ [Pipeline]}}
                        end
                    end);
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end;
create_pipeline(_) ->
    {error, invalid_pipeline}.

-spec list_pipelines() -> [raw_pipeline()].
list_pipelines() ->
    normalize_pipelines(entries(current_config(), ?PIPELINES)).

-spec lookup_pipeline(pipeline_id()) -> {ok, raw_pipeline()} | {error, not_found}.
lookup_pipeline(PipelineId) ->
    find_entry(pipeline_pred(PipelineId), list_pipelines()).

-spec update_pipeline(pipeline_id(), raw_pipeline()) ->
    {ok, raw_pipeline()} | {error, term()}.
update_pipeline(PipelineId, Body) when is_map(Body) ->
    case wrap_pipeline(put_entry_fields(Body, [{?PIPELINE_ID, PipelineId}])) of
        {ok, Pipeline} ->
            case
                update_raw_config(fun(Config0) ->
                    Pipelines0 = entries(Config0, ?PIPELINES),
                    case replace_entry(pipeline_pred(PipelineId), Pipeline, Pipelines0) of
                        {ok, Pipelines} -> {ok, Config0#{?PIPELINES => Pipelines}};
                        {error, _} = Error -> Error
                    end
                end)
            of
                ok -> lookup_pipeline(PipelineId);
                {error, _} = Error -> Error
            end;
        {error, _} = Error ->
            Error
    end;
update_pipeline(_, _) ->
    {error, invalid_pipeline}.

-spec delete_pipeline(pipeline_id()) -> ok | {error, term()}.
delete_pipeline(PipelineId) ->
    update_raw_config(fun(Config0) ->
        Pipelines0 = entries(Config0, ?PIPELINES),
        case remove_entry(pipeline_pred(PipelineId), Pipelines0) of
            {ok, Pipelines} -> {ok, Config0#{?PIPELINES => Pipelines}};
            {error, _} = Error -> Error
        end
    end).

%%--------------------------------------------------------------------
%% Internal config operations
%%--------------------------------------------------------------------

current_config() ->
    try emqx_plugins:get_config(name_vsn(), #{}) of
        {avro_value, _Type, _Value} = Config -> prepare_config_for_storage(avro_to_plain(Config));
        Config when is_map(Config) -> Config;
        _ -> #{}
    catch
        _:_ -> #{}
    end.

name_vsn() ->
    {ok, Vsn} = application:get_key(emqx_agent, vsn),
    iolist_to_binary([<<"emqx_agent-">>, Vsn]).

update_raw_config(Fun) ->
    Config0 = current_config(),
    case Fun(Config0) of
        {ok, Config} ->
            emqx_mgmt_api_plugins:put_plugin_config(name_vsn(), Config);
        {error, _} = Error ->
            Error
    end.

parse_config({avro_value, _Type, Value}) ->
    parse_config(avro_to_plain(Value));
parse_config(Config) when is_map(Config) ->
    {ok, normalize_config(Config)}.

cache_config(Parsed) ->
    persistent_term:put(?CONFIG_KEY, Parsed),
    ok.

entries(Config, Section) when is_map(Config) ->
    case maps:get(Section, Config, []) of
        Entries when is_list(Entries) -> Entries;
        _ -> []
    end.

required_id(Conn) when is_map(Conn), map_size(Conn) =:= 1 ->
    required_id(unwrap_union(Conn));
required_id(#{?CONNECTION_ID := Id}) when is_binary(Id), Id =/= <<>> ->
    {ok, Id};
required_id(#{?CONNECTION_ID := _}) ->
    {error, {invalid_field, ?CONNECTION_ID}};
required_id(_) ->
    {error, {missing_field, ?CONNECTION_ID}}.

required_skill_key(Skill) when is_map(Skill), map_size(Skill) =:= 1 ->
    required_skill_key(unwrap_union(Skill));
required_skill_key(#{?SKILL_TYPE := Type, ?SKILL_ID := SkillId}) when
    is_binary(Type), Type =/= <<>>, is_binary(SkillId), SkillId =/= <<>>
->
    {ok, Type, SkillId};
required_skill_key(#{?SKILL_TYPE := Type, ?SKILL_ID := _}) when
    not is_binary(Type); Type =:= <<>>
->
    {error, {invalid_field, ?SKILL_TYPE}};
required_skill_key(#{?SKILL_TYPE := _, ?SKILL_ID := _}) ->
    {error, {invalid_field, ?SKILL_ID}};
required_skill_key(#{?SKILL_ID := _}) ->
    {error, {missing_field, ?SKILL_TYPE}};
required_skill_key(#{?SKILL_TYPE := _}) ->
    {error, {missing_field, ?SKILL_ID}};
required_skill_key(_) ->
    {error, {missing_field, ?SKILL_TYPE}}.

required_pipeline_id(Pipeline) when is_map(Pipeline), map_size(Pipeline) =:= 1 ->
    required_pipeline_id(unwrap_union(Pipeline));
required_pipeline_id(#{?PIPELINE_ID := PipelineId}) when
    is_binary(PipelineId), PipelineId =/= <<>>
->
    {ok, PipelineId};
required_pipeline_id(#{?PIPELINE_ID := _}) ->
    {error, {invalid_field, ?PIPELINE_ID}};
required_pipeline_id(_) ->
    {error, {missing_field, ?PIPELINE_ID}}.

id_pred(Id) ->
    fun(Entry) -> maps:get(?CONNECTION_ID, unwrap_union(Entry), undefined) =:= Id end.

skill_pred(Type, SkillId) ->
    fun(Skill) ->
        Unwrapped = unwrap_union(Skill),
        maps:get(?SKILL_TYPE, Unwrapped, undefined) =:= Type andalso
            maps:get(?SKILL_ID, Unwrapped, undefined) =:= SkillId
    end.

pipeline_pred(PipelineId) ->
    fun(Pipeline) ->
        maps:get(?PIPELINE_ID, unwrap_union(Pipeline), undefined) =:= PipelineId
    end.

normalize_config(Config) ->
    Config1 = maybe_put(#{}, ?SKILLS, normalize_entries(maps:get(?SKILLS, Config, []))),
    Config2 = maybe_put(
        Config1, ?CONNECTIONS, normalize_entries(maps:get(?CONNECTIONS, Config, []))
    ),
    maybe_put(Config2, ?PIPELINES, normalize_pipelines(maps:get(?PIPELINES, Config, []))).

prepare_config_for_storage(Config) when is_map(Config) ->
    Config1 = maybe_put(
        #{}, ?SKILLS, [wrap_skill_for_storage(Entry) || Entry <- maps:get(?SKILLS, Config, [])]
    ),
    Config2 = maybe_put(
        Config1,
        ?CONNECTIONS,
        [wrap_connection(Entry) || Entry <- maps:get(?CONNECTIONS, Config, [])]
    ),
    maybe_put(
        Config2,
        ?PIPELINES,
        [wrap_pipeline_for_storage(Entry) || Entry <- maps:get(?PIPELINES, Config, [])]
    );
prepare_config_for_storage(_Config) ->
    #{}.

wrap_skill_for_storage(Entry) ->
    case wrap_skill(Entry) of
        {ok, Wrapped} -> Wrapped;
        {error, _} -> Entry
    end.

wrap_pipeline_for_storage(Entry) ->
    case wrap_pipeline(Entry) of
        {ok, Wrapped} -> Wrapped;
        {error, _} -> Entry
    end.

normalize_entries(Entries) when is_list(Entries) ->
    [unwrap_union(Entry) || Entry <- Entries];
normalize_entries(_) ->
    [].

normalize_pipelines(Pipelines) when is_list(Pipelines) ->
    [Pipeline || {ok, Pipeline} <- [normalize_pipeline(Pipeline0) || Pipeline0 <- Pipelines]];
normalize_pipelines(_) ->
    [].

normalize_pipeline(Pipeline0) when is_map(Pipeline0) ->
    Pipeline = unwrap_union(Pipeline0),
    case Pipeline of
        #{<<"steps">> := Steps} when is_list(Steps) ->
            case normalize_steps(Steps, 1, []) of
                {ok, NormalizedSteps} -> {ok, Pipeline#{<<"steps">> => NormalizedSteps}};
                {error, _} = Error -> Error
            end;
        #{} ->
            {ok, Pipeline}
    end;
normalize_pipeline(_) ->
    {error, invalid_pipeline}.

normalize_steps([], _Index, Acc) ->
    {ok, lists:reverse(Acc)};
normalize_steps([Step0 | Rest], Index, Acc) when is_map(Step0) ->
    Step1 = unwrap_union(Step0),
    case normalize_step(Step1, Index) of
        {ok, Step} -> normalize_steps(Rest, Index + 1, [Step | Acc]);
        {error, _} = Error -> Error
    end;
normalize_steps([_ | _], Index, _Acc) ->
    {error, {invalid_step, Index}}.

normalize_step(#{<<"type">> := <<"llm_loop">>} = Step0, _Index) ->
    Step1 = name_value_entries_to_map(<<"input">>, Step0),
    {ok, decode_json_schema_field(<<"set_result_schema">>, Step1)};
normalize_step(#{<<"type">> := <<"break">>} = Step0, _Index) ->
    {ok, normalize_break_step(Step0)};
normalize_step(Step, _Index) ->
    {ok, name_value_entries_to_map(<<"args">>, Step)}.

normalize_break_step(#{<<"eq">> := <<"true">>} = Step) ->
    Step#{<<"eq">> => true};
normalize_break_step(#{<<"eq">> := <<"false">>} = Step) ->
    Step#{<<"eq">> => false};
normalize_break_step(Step) ->
    Step.

name_value_entries_to_map(Field, Step) ->
    case maps:get(Field, Step, undefined) of
        Entries when is_list(Entries) ->
            Step#{Field => maps:from_list([name_value_entry_to_pair(E) || E <- Entries])};
        _ ->
            Step
    end.

name_value_entry_to_pair(#{<<"name">> := Name, <<"value">> := Value}) ->
    {Name, Value};
name_value_entry_to_pair(#{name := Name, value := Value}) ->
    {Name, Value}.

decode_json_schema_field(Field, Step) ->
    case maps:get(Field, Step, undefined) of
        Value when is_binary(Value) ->
            Step#{Field => emqx_agent_oai_tool_schema:json_schema_from_string(Value, [])};
        _ ->
            Step
    end.

unwrap_union(Map) when is_map(Map), map_size(Map) =:= 1 ->
    case maps:to_list(Map) of
        [{Key, Value}] when is_binary(Key), is_map(Value) ->
            Value;
        _ ->
            Map
    end;
unwrap_union(Value) ->
    Value.

avro_to_plain([{Key, _Value} | _] = Fields) when is_binary(Key) ->
    maps:from_list([{K, avro_to_plain(V)} || {K, V} <- Fields]);
avro_to_plain({avro_value, _Type, Value}) ->
    avro_to_plain(Value);
avro_to_plain(Values) when is_list(Values) ->
    [avro_to_plain(Value) || Value <- Values];
avro_to_plain(Value) ->
    Value.

put_entry_fields(Entry, Fields) when is_map(Entry), map_size(Entry) =:= 1 ->
    case maps:to_list(Entry) of
        [{Key, Value}] when is_binary(Key), is_map(Value) ->
            #{Key => put_entry_fields(Value, Fields)};
        _ ->
            put_fields(Entry, Fields)
    end;
put_entry_fields(Entry, Fields) ->
    put_fields(Entry, Fields).

put_fields(Entry, Fields) ->
    lists:foldl(fun({Key, Value}, Acc) -> Acc#{Key => Value} end, Entry, Fields).

wrap_skill(Entry0) ->
    Entry = unwrap_union(Entry0),
    Type = maps:get(?SKILL_TYPE, Entry, undefined),
    RecordName = skill_record_name(Type),
    case skill_record_exists(RecordName) of
        true -> {ok, #{RecordName => Entry}};
        false -> {error, unknown_type}
    end.

skill_record_exists(RecordName) when is_binary(RecordName) ->
    lists:member(RecordName, skill_record_names());
skill_record_exists(_) ->
    false.

skill_record_names() ->
    Schema = config_schema(),
    #{<<"type">> := #{<<"items">> := Items}} = field_schema(?SKILLS, Schema),
    [Name || #{<<"type">> := <<"record">>, <<"name">> := Name} <- Items].

field_schema(Name, #{<<"fields">> := Fields}) ->
    hd([Field || #{<<"name">> := FieldName} = Field <- Fields, FieldName =:= Name]).

read_config_schema() ->
    File = filename:join(code:priv_dir(emqx_agent), "config_schema.avsc"),
    {ok, Bin} = file:read_file(File),
    emqx_utils_json:decode(Bin).

skill_record_name(Type) when is_binary(Type) ->
    Parts = binary:split(Type, <<"__">>, [global]),
    iolist_to_binary(lists:join(<<"_">>, [camel_part(Part) || Part <- Parts]));
skill_record_name(Type) ->
    Type.

camel_part(Part) ->
    Words = binary:split(Part, <<"_">>, [global]),
    iolist_to_binary([capitalize_word(Word) || Word <- Words]).

capitalize_word(<<First, Rest/binary>>) ->
    <<(uppercase(First)), Rest/binary>>;
capitalize_word(<<>>) ->
    <<>>.

uppercase(Char) when Char >= $a, Char =< $z ->
    Char - 32;
uppercase(Char) ->
    Char.

wrap_connection(Entry0) ->
    Entry = unwrap_union(Entry0),
    case maps:get(?SKILL_TYPE, Entry, undefined) of
        <<"postgresql">> -> #{<<"ConnectionPostgresql">> => Entry};
        _ -> Entry0
    end.

wrap_pipeline(Entry0) ->
    case normalize_pipeline(Entry0) of
        {ok, _} -> {ok, prepare_pipeline_for_config(unwrap_union(Entry0))};
        {error, _} = Error -> Error
    end.

prepare_pipeline_for_config(#{<<"steps">> := Steps} = Pipeline) when is_list(Steps) ->
    Pipeline#{<<"steps">> => [wrap_pipeline_step(Step) || Step <- Steps]};
prepare_pipeline_for_config(Pipeline) ->
    Pipeline.

wrap_pipeline_step(Step0) ->
    Step1 = unwrap_union(Step0),
    Step = prepare_step_for_config(Step1),
    case pipeline_step_record_name(maps:get(?SKILL_TYPE, Step, undefined)) of
        RecordName when is_binary(RecordName) -> #{RecordName => Step};
        _ -> Step0
    end.

pipeline_step_record_name(Type) when is_binary(Type) ->
    <<"PipelineStep", (skill_record_name(Type))/binary>>;
pipeline_step_record_name(Type) ->
    Type.

prepare_step_for_config(#{<<"type">> := <<"call_skill">>, <<"args">> := Args} = Step) when
    is_map(Args)
->
    Step#{<<"args">> => map_to_name_value_entries(Args)};
prepare_step_for_config(#{<<"type">> := <<"llm_loop">>} = Step0) ->
    Step1 =
        case maps:get(<<"input">>, Step0, undefined) of
            Input when is_map(Input) -> Step0#{<<"input">> => map_to_name_value_entries(Input)};
            _ -> Step0
        end,
    case maps:get(<<"set_result_schema">>, Step1, undefined) of
        Schema when is_map(Schema) ->
            Step1#{<<"set_result_schema">> => emqx_utils_json:encode(Schema)};
        _ ->
            Step1
    end;
prepare_step_for_config(Step) ->
    Step.

map_to_name_value_entries(Map) ->
    [#{<<"name">> => Name, <<"value">> => Value} || {Name, Value} <- maps:to_list(Map)].

maybe_put(Map, _Key, []) ->
    Map;
maybe_put(Map, Key, Value) ->
    Map#{Key => Value}.

path_key(Key) when is_atom(Key) -> atom_to_binary(Key, utf8);
path_key(Key) -> Key.

find_entry(Pred, Entries) ->
    case lists:filter(Pred, Entries) of
        [Entry | _] -> {ok, Entry};
        [] -> {error, not_found}
    end.

replace_entry(Pred, Entry, Entries) ->
    replace_entry(Pred, Entry, Entries, []).

replace_entry(_Pred, _Entry, [], _Acc) ->
    {error, not_found};
replace_entry(Pred, Entry, [Head | Rest], Acc) ->
    case Pred(Head) of
        true -> {ok, lists:reverse(Acc) ++ [Entry | Rest]};
        false -> replace_entry(Pred, Entry, Rest, [Head | Acc])
    end.

remove_entry(Pred, Entries) ->
    remove_entry(Pred, Entries, []).

remove_entry(_Pred, [], _Acc) ->
    {error, not_found};
remove_entry(Pred, [Head | Rest], Acc) ->
    case Pred(Head) of
        true -> {ok, lists:reverse(Acc) ++ Rest};
        false -> remove_entry(Pred, Rest, [Head | Acc])
    end.
