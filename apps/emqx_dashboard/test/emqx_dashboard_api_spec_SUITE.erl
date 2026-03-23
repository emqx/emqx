%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_api_spec_SUITE).

-moduledoc "Common Test suite for the dashboard API spec explorer endpoints.".

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(HOST, "http://127.0.0.1:18083").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

-doc "Verify /api-spec.md returns markdown with expected content-type and body.".
t_index_markdown(_Config) ->
    {200, Headers, Body} = do_get_raw("/api-spec.md"),
    ContentType = proplists:get_value("content-type", Headers, ""),
    ?assert(string:find(list_to_binary(ContentType), <<"text/markdown">>) =/= nomatch),
    BodyBin = list_to_binary(Body),
    ?assert(string:find(BodyBin, <<"# EMQX HTTP API">>) =/= nomatch),
    ?assert(
        string:find(BodyBin, <<"Drill-down endpoints return Swagger 3.0 JSON specs.">>) =/= nomatch
    ),
    ?assert(string:find(BodyBin, <<"POST /api/v5/login">>) =/= nomatch).

-doc "Verify /api-spec without a suffix returns 404.".
t_index_without_suffix_not_found(_Config) ->
    {404, _, _} = do_get_raw("/api-spec").

-doc "Verify /api-spec.json returns a structured index with tags, drill_down, api_auth, and bootstrap_help.".
t_index_json(_Config) ->
    {200, Body} = do_get("/api-spec.json"),
    ?assertMatch(
        #{<<"tags">> := _, <<"drill_down">> := _, <<"api_auth">> := _, <<"bootstrap_help">> := _},
        Body
    ),
    ?assertMatch([#{<<"title">> := _, <<"text">> := _} | _], maps:get(<<"bootstrap_help">>, Body)),
    Tags = maps:get(<<"tags">>, Body),
    ?assert(map_size(Tags) > 0),
    lists:foreach(
        fun(TagInfo) ->
            ?assertMatch(#{<<"url">> := _}, TagInfo),
            Url = maps:get(<<"url">>, TagInfo),
            ?assertMatch(<<"/api-spec/", _/binary>>, Url),
            ?assertEqual(Url, string:lowercase(Url))
        end,
        maps:values(Tags)
    ),
    DrillDown = maps:get(<<"drill_down">>, Body),
    lists:foreach(
        fun(Key) ->
            ?assert(maps:is_key(Key, DrillDown), #{missing_key => Key})
        end,
        [
            <<"actions">>,
            <<"sources">>,
            <<"connectors">>,
            <<"authentication">>,
            <<"authorization">>,
            <<"listeners">>
        ]
    ).

-doc "Verify api-spec.html exists in priv dir and contains the expected title.".
t_api_spec_html(_Config) ->
    HtmlPath = filename:join([code:priv_dir(emqx_dashboard), "api-spec.html"]),
    ?assertMatch({ok, _}, file:read_file_info(HtmlPath)),
    {ok, Body} = file:read_file(HtmlPath),
    ?assert(string:find(Body, <<"EMQX API Spec Explorer">>) =/= nomatch).

-doc "Verify /api-spec/:tag returns a valid OpenAPI 3.0.0 doc with correctly tagged paths.".
t_tag_spec(_Config) ->
    %% "status" is a tag always present when emqx_management is loaded.
    {200, Spec} = do_get("/api-spec/status"),
    assert_openapi_shape(Spec),
    %% All paths in the filtered spec must be tagged with "Status".
    Paths = maps:get(<<"paths">>, Spec),
    ?assert(map_size(Paths) > 0),
    maps:foreach(
        fun(_Path, PathItem) ->
            maps:foreach(
                fun
                    (_Method, OpSpec) when is_map(OpSpec) ->
                        Tags = maps:get(<<"tags">>, OpSpec, []),
                        ?assert(lists:member(<<"Status">>, Tags), #{path => _Path, tags => Tags});
                    (_Method, _NonMap) ->
                        ok
                end,
                PathItem
            )
        end,
        Paths
    ),
    ?assertNot(maps:is_key(<<"x-operations">>, Spec)).

-doc "Verify tags without resource IDs (like 'status') omit x-resource-info.".
t_tag_spec_no_resource_info_for_status(_Config) ->
    {200, Spec} = do_get("/api-spec/status"),
    ?assertNot(maps:is_key(<<"x-resource-info">>, Spec)).

-doc "Verify a per-tag spec has fewer schemas than the full swagger spec.".
t_tag_spec_schemas_trimmed(_Config) ->
    {200, FullSpec} = do_get("/api-docs/swagger.json"),
    FullSchemas = schema_count(FullSpec),
    {200, TagSpec} = do_get("/api-spec/status"),
    TagSchemas = schema_count(TagSpec),
    ct:pal("Full swagger schemas: ~p, status tag schemas: ~p", [FullSchemas, TagSchemas]),
    ?assert(TagSchemas < FullSchemas).

-doc "Verify /api-spec/:unknown_tag returns 404.".
t_tag_not_found(_Config) ->
    {404, _} = do_get("/api-spec/this_tag_does_not_exist_at_all").

-doc "Verify /api-spec/:tag/:name returns a valid OpenAPI doc with a schema subset.".
t_drilldown_spec(_Config) ->
    {200, TagSpec} = do_get("/api-spec/status"),
    %% Pick the first schema name from the tag spec and use it as the drill-down name.
    Schemas = maps:get(<<"schemas">>, maps:get(<<"components">>, TagSpec, #{}), #{}),
    case maps:keys(Schemas) of
        [] ->
            ct:pal("No schemas in status tag spec; skipping drill-down test"),
            ok;
        [FirstSchema | _] ->
            %% Use the lowercase schema name as the drill-down key.
            Name = string:lowercase(FirstSchema),
            Url = "/api-spec/status/" ++ binary_to_list(Name),
            {200, DrillSpec} = do_get(Url),
            assert_openapi_shape(DrillSpec),
            DrillSchemas = schema_count(DrillSpec),
            TagSchemas = schema_count(TagSpec),
            ct:pal(
                "Tag schemas: ~p, drill-down schemas for ~p: ~p",
                [TagSchemas, Name, DrillSchemas]
            )
    end.

-doc "Verify /api-spec/:tag/:unknown_name returns 404.".
t_drilldown_not_found(_Config) ->
    {404, _} = do_get("/api-spec/status/this_name_definitely_does_not_exist_xyz_abc").

-doc "Verify /api-spec/actions/kafka narrows examples to kafka-related entries only.".
t_drilldown_filters_examples(_Config) ->
    case do_get("/api-spec/actions/kafka") of
        {200, Spec} ->
            Examples = action_post_examples(Spec),
            ?assert(is_map(Examples)),
            ?assert(map_size(Examples) > 0),
            maps:foreach(
                fun(Key, _Val) ->
                    ?assert(
                        string:find(string:lowercase(Key), <<"kafka">>) =/= nomatch,
                        #{example_key => Key}
                    )
                end,
                Examples
            );
        {404, _} ->
            ct:pal("actions tag not present, skipping"),
            ok
    end.

-doc "Verify /api-spec/actions/kafka_producer is accepted as an exact drill-down name.".
t_drilldown_exact_producer_name(_Config) ->
    case do_get("/api-spec/actions/kafka_producer") of
        {200, Spec} ->
            Examples = action_post_examples(Spec),
            ?assert(is_map(Examples)),
            ?assert(maps:is_key(<<"kafka_producer">>, Examples));
        {404, _} ->
            ct:pal("kafka_producer drill-down not present, skipping"),
            ok
    end.

-doc "Verify connectors/kafka_producer drill-down trims schemas to a reasonable count with kafka-related names.".
t_drilldown_connector_kafka_producer_schema_trim(_Config) ->
    case do_get("/api-spec/connectors/kafka_producer") of
        {200, Spec} ->
            Schemas = maps:get(<<"schemas">>, maps:get(<<"components">>, Spec, #{}), #{}),
            SchemaNames = maps:keys(Schemas),
            ?assert(length(SchemaNames) < 80),
            ?assert(
                lists:any(
                    fun(Name) ->
                        string:find(string:lowercase(Name), <<"kafka">>) =/= nomatch
                    end,
                    SchemaNames
                )
            );
        {404, _} ->
            ct:pal("connectors/kafka_producer drill-down not present, skipping"),
            ok
    end.

-doc "Verify listeners/tcp drill-down returns a valid spec with tcp-related schemas.".
t_drilldown_listeners_tcp(_Config) ->
    case do_get("/api-spec/listeners/tcp") of
        {200, Spec} ->
            assert_openapi_shape(Spec),
            Schemas = maps:get(<<"schemas">>, maps:get(<<"components">>, Spec, #{}), #{}),
            SchemaNames = maps:keys(Schemas),
            ?assert(
                lists:any(
                    fun(Name) ->
                        string:find(string:lowercase(Name), <<"tcp">>) =/= nomatch
                    end,
                    SchemaNames
                )
            );
        {404, _} ->
            ct:pal("listeners/tcp drill-down not present, skipping"),
            ok
    end.

-doc "Verify drill-down specs reduce oneOf sizes or schema counts vs the full tag spec.".
t_drilldown_no_full_union_fallback(_Config) ->
    Categories = [
        {<<"actions">>, action_names()},
        {<<"sources">>, source_names()},
        {<<"connectors">>, connector_names()},
        {<<"authentication">>, authn_names()},
        {<<"authorization">>, authz_names()}
    ],
    lists:foreach(
        fun({Category, Names}) ->
            case do_get("/api-spec/" ++ binary_to_list(Category)) of
                {200, TagSpec} ->
                    FullMaxOneOf = max_oneof_size(TagSpec),
                    lists:foreach(
                        fun(Name) ->
                            Url =
                                "/api-spec/" ++ binary_to_list(Category) ++ "/" ++
                                    binary_to_list(Name),
                            case do_get(Url) of
                                {200, DrillSpec} ->
                                    TagSchemas = schema_count(TagSpec),
                                    DrillSchemas = schema_count(DrillSpec),
                                    DrillMaxOneOf = max_oneof_size(DrillSpec),
                                    case FullMaxOneOf > 1 of
                                        true ->
                                            ?assert(
                                                DrillMaxOneOf < FullMaxOneOf orelse
                                                    DrillSchemas < TagSchemas,
                                                #{
                                                    category => Category,
                                                    name => Name,
                                                    full_max_oneof => FullMaxOneOf,
                                                    drilldown_max_oneof => DrillMaxOneOf,
                                                    tag_schemas => TagSchemas,
                                                    drilldown_schemas => DrillSchemas
                                                }
                                            );
                                        false ->
                                            ok
                                    end;
                                {404, _Body} ->
                                    ?assert(
                                        false,
                                        #{
                                            category => Category,
                                            name => Name,
                                            reason => <<"drilldown_404">>
                                        }
                                    )
                            end
                        end,
                        Names
                    );
                {404, _} ->
                    ct:pal("Category ~p not present in current app set, skipping", [Category])
            end
        end,
        Categories
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

do_get_raw(Path) ->
    Url = ?HOST ++ Path,
    case httpc:request(get, {Url, []}, [], []) of
        {ok, {{_, Code, _}, Headers, RawBody}} ->
            {Code, Headers, RawBody};
        {error, Reason} ->
            error({http_request_failed, Reason})
    end.

do_get(Path) ->
    Url = ?HOST ++ Path,
    case httpc:request(get, {Url, []}, [], []) of
        {ok, {{_, Code, _}, _Headers, RawBody}} ->
            Body =
                try
                    emqx_utils_json:decode(list_to_binary(RawBody))
                catch
                    _:_ -> RawBody
                end,
            {Code, Body};
        {error, Reason} ->
            error({http_request_failed, Reason})
    end.

assert_openapi_shape(Spec) ->
    ?assertMatch(#{<<"openapi">> := <<"3.0.0">>}, Spec),
    ?assert(maps:is_key(<<"info">>, Spec)),
    ?assert(maps:is_key(<<"paths">>, Spec)),
    ?assert(maps:is_key(<<"components">>, Spec)).

schema_count(Spec) ->
    Components = maps:get(<<"components">>, Spec, #{}),
    Schemas = maps:get(<<"schemas">>, Components, #{}),
    map_size(Schemas).

action_post_examples(Spec) ->
    Paths = maps:get(<<"paths">>, Spec, #{}),
    Actions = maps:get(<<"/actions">>, Paths, #{}),
    Post = maps:get(<<"post">>, Actions, #{}),
    ReqBody = maps:get(<<"requestBody">>, Post, #{}),
    Content = maps:get(<<"content">>, ReqBody, #{}),
    AppJson = maps:get(<<"application/json">>, Content, #{}),
    maps:get(<<"examples">>, AppJson, #{}).

max_oneof_size(Term) when is_map(Term) ->
    Local =
        case maps:find(<<"oneOf">>, Term) of
            {ok, OneOf} when is_list(OneOf) -> length(OneOf);
            _ -> 0
        end,
    maps:fold(fun(_K, V, Acc) -> erlang:max(Acc, max_oneof_size(V)) end, Local, Term);
max_oneof_size(Term) when is_list(Term) ->
    lists:foldl(fun(V, Acc) -> erlang:max(Acc, max_oneof_size(V)) end, 0, Term);
max_oneof_size(_Term) ->
    0.

connector_names() ->
    safe_call(fun() ->
        Types = emqx_connector_info:connector_types(),
        lists:sort([atom_to_binary(T, utf8) || T <- Types])
    end).

action_names() ->
    safe_call(fun() ->
        Pairs = emqx_action_info:registered_schema_modules_actions(),
        lists:sort([atom_to_binary(T, utf8) || {T, _} <- Pairs])
    end).

source_names() ->
    safe_call(fun() ->
        Pairs = emqx_action_info:registered_schema_modules_sources(),
        lists:sort([atom_to_binary(T, utf8) || {T, _} <- Pairs])
    end).

authn_names() ->
    safe_call(fun() ->
        UnionType = emqx_authn_schema:authenticator_type(),
        {union, Selector, _} = UnionType,
        Refs = hoconsc:union_members(Selector),
        Names = lists:usort([ref_schema_name(R) || R <- Refs, ref_schema_name(R) =/= undefined]),
        lists:sort(Names)
    end).

authz_names() ->
    safe_call(fun() ->
        Types = emqx_authz_schema:source_types(),
        lists:sort([atom_to_binary(T, utf8) || T <- Types])
    end).

listener_names() ->
    safe_call(fun() ->
        Types = emqx_schema:listeners(),
        lists:sort([list_to_binary(Type) || {Type, _} <- Types])
    end).

ref_schema_name({ref, _Mod, Name}) when is_atom(Name) ->
    atom_to_binary(Name, utf8);
ref_schema_name({ref, _Mod, Name}) when is_list(Name) ->
    list_to_binary(Name);
ref_schema_name({ref, _Mod, Name}) when is_binary(Name) ->
    Name;
ref_schema_name(_) ->
    undefined.

safe_call(Fun) ->
    try
        Fun()
    catch
        _:_ -> []
    end.
