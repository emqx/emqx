%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% PostgreSQL query skill.
%%
%% Invoke topic:  cap/postgresql.query/<id>/request/<req_id>
%% Reply topic:   cap/postgresql.query/<id>/response/<req_id>
%%
%% The module owns a single shared PostgreSQL resource with fixed configuration.
%% Skill instances differ by SQL query template and schemas only.

-module(emqx_agent_skill_postgresql).

-define(SKILL_TYPE, <<"postgresql.query">>).
-define(RESOURCE_ID, <<"emqx_agent_skill_postgresql_resource">>).
-define(RESOURCE_GROUP, <<"emqx_agent">>).

-export([init/0, deinit/0, create/1, destroy/1, to_map/1, resource_id/0, handle_invoke/2]).

-spec resource_id() -> binary().
resource_id() ->
    ?RESOURCE_ID.

-spec init() -> ok.
init() ->
    ok = emqx_agent_skill_registry:register_type(?SKILL_TYPE, ?MODULE),
    ok = ensure_resource_if_available(),
    ok.

-spec deinit() -> ok.
deinit() ->
    ok = emqx_agent_skill_registry:unregister_type(?SKILL_TYPE),
    ok = emqx_resource:remove_local(?RESOURCE_ID),
    ok.

-spec create(Context :: map()) -> ok | {error, term()}.
create(#{skill_id := SkillId, desc := Desc, query := Query} = Context) ->
    ok = ensure_resource_if_available(),
    {ParsedQuery, RowTemplate, VarNames} = parse_query(Query),
    InSchema = input_schema(VarNames),
    SkillContext = Context#{
        parsed_query => ParsedQuery,
        row_template => RowTemplate,
        var_names => VarNames
    },
    Skill = #{
        skill_id => SkillId,
        type => ?SKILL_TYPE,
        module => ?MODULE,
        display_name => <<"PostgreSQL Query">>,
        description => Desc,
        context => SkillContext,
        input_schema => InSchema
    },
    emqx_agent_skill_registry:register(Skill).

-spec destroy(emqx_agent_skill_registry:skill_id()) -> ok.
destroy(SkillId) ->
    emqx_agent_skill_registry:unregister(?SKILL_TYPE, SkillId).

-spec to_map(map()) -> map().
to_map(#{
    skill_id := Id, description := Desc, context := Ctx, input_schema := In
}) ->
    #{
        <<"skill_id">> => Id,
        <<"type">> => ?SKILL_TYPE,
        <<"description">> => Desc,
        <<"query">> => maps:get(query, Ctx, <<>>),
        <<"input_schema">> => In
    }.

handle_invoke(Context, Request) ->
    do_reply(Context, Request).

do_reply(Context, Request) ->
    Args = maps:get(<<"args">>, Request, #{}),
    Query = maps:get(parsed_query, Context, maps:get(query, Context, <<>>)),
    RowTemplate = maps:get(row_template, Context, []),
    Params = render_params(RowTemplate, Args),
    case run_query(Query, Params) of
        {ok, Rows} ->
            {ok, #{<<"rows">> => Rows}};
        {error, Reason} ->
            {error, iolist_to_binary(io_lib:format("~0p", [Reason]))}
    end.

run_query(Query, []) ->
    ok = ensure_resource(),
    case emqx_resource:simple_sync_query(?RESOURCE_ID, {query, Query}) of
        {ok, Cols, Rows} -> {ok, rows_to_maps(Cols, Rows)};
        {ok, _, Cols, Rows} -> {ok, rows_to_maps(Cols, Rows)};
        {ok, _RowCount} -> {ok, []};
        Error -> Error
    end;
run_query(Query, Params) ->
    ok = ensure_resource(),
    case emqx_resource:simple_sync_query(?RESOURCE_ID, {query, Query, Params}) of
        {ok, Cols, Rows} -> {ok, rows_to_maps(Cols, Rows)};
        {ok, _, Cols, Rows} -> {ok, rows_to_maps(Cols, Rows)};
        {ok, _RowCount} -> {ok, []};
        Error -> Error
    end.

parse_query(Query) ->
    {ParsedQuery, RowTemplate} = emqx_template_sql:parse_prepstmt(Query, #{parameters => '$n'}),
    VarNames = [to_binary(Name) || Name <- lists:usort(emqx_template:placeholders(RowTemplate))],
    {iolist_to_binary(ParsedQuery), RowTemplate, VarNames}.

input_schema([]) ->
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => #{},
        <<"required">> => [],
        <<"additionalProperties">> => false
    };
input_schema(VarNames) ->
    Props = maps:from_list([{Name, #{<<"type">> => <<"string">>}} || Name <- VarNames]),
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => Props,
        <<"required">> => VarNames,
        <<"additionalProperties">> => false
    }.

render_params([], _Args) ->
    [];
render_params(RowTemplate, Args) ->
    {Params, _Errors} = emqx_template_sql:render_prepstmt(RowTemplate, Args),
    Params.

ensure_resource() ->
    case
        emqx_resource:create_local(
            ?RESOURCE_ID,
            ?RESOURCE_GROUP,
            emqx_agent_skill_postgresql_connector,
            pgsql_config(),
            #{
                health_check_interval => 1000,
                start_timeout => 5000,
                start_after_created => true
            }
        )
    of
        {ok, _} -> ok;
        {error, {already_exists, _}} -> ok;
        {error, {resource_id_already_exist, _}} -> ok;
        {error, {bad_resource_config, #{reason := already_exists}}} -> ok;
        {error, Reason} -> {error, Reason}
    end.

ensure_resource_if_available() ->
    case whereis(emqx_resource_manager_sup) of
        undefined -> ok;
        _ -> ensure_resource()
    end.

rows_to_maps(Cols, Rows) ->
    Names = [column_name(C) || C <- Cols],
    [
        maps:from_list(lists:zip(Names, [normalize_value(V) || V <- tuple_to_list(Row)]))
     || Row <- Rows
    ].

column_name(#{name := Name}) ->
    to_binary(Name);
column_name({column, _TOid, _TAttr, Name, _Type, _Size, _Mod, _Fmt}) ->
    to_binary(Name);
column_name({column, Name, _Type, _Oid, _Size, _Mod, _Fmt, _TableOid, _AttrNum}) ->
    to_binary(Name);
column_name(Other) ->
    iolist_to_binary(io_lib:format("~0p", [Other])).

normalize_value(V) when is_binary(V) -> V;
normalize_value(V) when is_integer(V) -> V;
normalize_value(V) when is_float(V) -> V;
normalize_value(V) when is_boolean(V) -> V;
normalize_value(null) -> null;
normalize_value(V) when is_list(V) -> unicode:characters_to_binary(V);
normalize_value(V) -> iolist_to_binary(io_lib:format("~0p", [V])).

to_binary(V) when is_binary(V) -> V;
to_binary(V) when is_list(V) -> unicode:characters_to_binary(V);
to_binary(V) when is_atom(V) -> atom_to_binary(V, utf8).

pgsql_config() ->
    #{
        auto_reconnect => true,
        connect_timeout => 5000,
        disable_prepared_statements => true,
        database => <<"mqtt">>,
        username => <<"root">>,
        password => <<"public">>,
        pool_size => 1,
        server => <<"pgsql">>,
        ssl => #{enable => false}
    }.
