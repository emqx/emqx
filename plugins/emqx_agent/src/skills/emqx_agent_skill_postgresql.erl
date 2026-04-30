%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% PostgreSQL query skill.
%%
%% Invoke topic:  cap/postgresql.query/<id>/request
%% Reply topic:   cap/postgresql.query/<id>/response/<req_id>
%%
%% The module owns a single shared PostgreSQL resource with fixed configuration.
%% Skill instances differ by SQL query template and schemas only.

-module(emqx_agent_skill_postgresql).

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(SKILL_TYPE, <<"postgresql.query">>).
-define(RESOURCE_ID, <<"emqx_agent_skill_postgresql_resource">>).
-define(RESOURCE_GROUP, <<"emqx_agent">>).

-export([init/0, deinit/0, create/1, destroy/1, to_map/1, resource_id/0]).
-export([maybe_expand_schema/2]).
-export([on_message_publish/1]).

-spec resource_id() -> binary().
resource_id() ->
    ?RESOURCE_ID.

-spec init() -> ok.
init() ->
    _ = emqx_hooks:add('message.publish', {?MODULE, on_message_publish, []}, ?HP_LOWEST),
    ok = ensure_resource_if_available(),
    ok.

-spec deinit() -> ok.
deinit() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    ok = emqx_resource:remove_local(?RESOURCE_ID),
    ok.

-spec create(Context :: map()) -> ok | {error, term()}.
create(#{skill_id := SkillId, desc := Desc, query := _Query} = Context) ->
    ok = ensure_resource_if_available(),
    ArgKeys = maps:get(arg_keys, Context, []),
    RawInSchema = maps:get(input_schema, Context, #{<<"type">> => <<"object">>}),
    InSchema = maybe_expand_schema(RawInSchema, ArgKeys),
    OutSchema = maps:get(output_schema, Context, #{<<"type">> => <<"object">>}),
    Skill = #{
        skill_id => SkillId,
        type => ?SKILL_TYPE,
        display_name => <<"PostgreSQL Query">>,
        description => Desc,
        context => Context,
        input_schema => InSchema,
        output_schema => OutSchema
    },
    emqx_agent_skill_registry:register(Skill).

%% If the provided schema already has properties, use it as-is.
%% Otherwise auto-generate string properties from arg_keys.
-spec maybe_expand_schema(map(), [binary()]) -> map().
maybe_expand_schema(#{<<"properties">> := _} = Schema, _ArgKeys) ->
    Schema;
maybe_expand_schema(_, []) ->
    #{<<"type">> => <<"object">>};
maybe_expand_schema(_, ArgKeys) ->
    Props = maps:from_list([{K, #{<<"type">> => <<"string">>}} || K <- ArgKeys]),
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => Props,
        <<"required">> => ArgKeys
    }.

-spec destroy(emqx_agent_skill_registry:skill_id()) -> ok.
destroy(SkillId) ->
    emqx_agent_skill_registry:unregister(?SKILL_TYPE, SkillId).

-spec to_map(map()) -> map().
to_map(#{
    skill_id := Id, description := Desc, context := Ctx, input_schema := In, output_schema := Out
}) ->
    #{
        <<"skill_id">> => Id,
        <<"type">> => ?SKILL_TYPE,
        <<"description">> => Desc,
        <<"query">> => maps:get(query, Ctx, <<>>),
        <<"arg_keys">> => maps:get(arg_keys, Ctx, []),
        <<"input_schema">> => In,
        <<"output_schema">> => Out
    }.

on_message_publish(Msg) ->
    emqx_agent_skill_helpers:if_skill_request(
        ?SKILL_TYPE,
        fun(SkillId, #message{payload = Payload}) ->
            handle_invoke(SkillId, Payload)
        end,
        Msg
    ).

handle_invoke(SkillId, Payload) ->
    case emqx_agent_skill_registry:lookup(?SKILL_TYPE, SkillId) of
        {error, not_found} ->
            ok;
        {ok, #{context := Context}} ->
            Request = emqx_utils_json:decode(Payload),
            do_reply(SkillId, Context, Request)
    end.

do_reply(SkillId, Context, Request) ->
    Args = maps:get(<<"args">>, Request, #{}),
    Query = maps:get(query, Context, <<>>),
    ArgKeys = maps:get(arg_keys, Context, []),
    Params = [maps:get(K, Args, null) || K <- ArgKeys],
    Data =
        case run_query(Query, Params) of
            {ok, Rows} ->
                #{<<"status">> => <<"ok">>, <<"rows">> => Rows};
            {error, Reason} ->
                #{
                    <<"status">> => <<"error">>,
                    <<"reason">> => iolist_to_binary(io_lib:format("~0p", [Reason]))
                }
        end,
    emqx_agent_skill_helpers:publish_reply(?SKILL_TYPE, SkillId, Request, Data).

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
