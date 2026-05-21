%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_pipeline_ctx_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_resolve_top_level(_Config) ->
    Ctx = #{<<"event">> => #{<<"id">> => <<"x1">>}},
    ?assertEqual(#{<<"id">> => <<"x1">>}, emqx_agent_pipeline_ctx:resolve(<<"$.event">>, Ctx)).

t_resolve_deep_path(_Config) ->
    Ctx = #{<<"foo">> => #{<<"bar">> => #{<<"baz">> => 42}}},
    ?assertEqual(42, emqx_agent_pipeline_ctx:resolve(<<"$.foo.bar.baz">>, Ctx)).

t_resolve_non_path_passthrough(_Config) ->
    ?assertEqual(<<"hello">>, emqx_agent_pipeline_ctx:resolve(<<"hello">>, #{})).

t_resolve_missing_path_returns_null(_Config) ->
    Ctx = #{<<"foo">> => #{}},
    ?assertEqual(null, emqx_agent_pipeline_ctx:resolve(<<"$.foo.missing">>, Ctx)).

t_resolve_missing_top_level_returns_null(_Config) ->
    ?assertEqual(null, emqx_agent_pipeline_ctx:resolve(<<"$.nonexistent">>, #{})).

t_write_single_segment(_Config) ->
    Result = emqx_agent_pipeline_ctx:write(<<"$.key">>, <<"val">>, #{}),
    ?assertEqual(#{<<"key">> => <<"val">>}, Result).

t_write_deep_nested(_Config) ->
    Result = emqx_agent_pipeline_ctx:write(<<"$.foo.bar">>, <<"val">>, #{<<"foo">> => #{}}),
    ?assertEqual(#{<<"foo">> => #{<<"bar">> => <<"val">>}}, Result).

t_write_creates_intermediate_maps(_Config) ->
    Result = emqx_agent_pipeline_ctx:write(<<"$.a.b.c">>, 1, #{}),
    ?assertEqual(#{<<"a">> => #{<<"b">> => #{<<"c">> => 1}}}, Result).

t_write_undefined_is_noop(_Config) ->
    Ctx = #{<<"x">> => 1},
    ?assertEqual(Ctx, emqx_agent_pipeline_ctx:write(undefined, <<"val">>, Ctx)).

t_write_non_dollar_path_is_noop(_Config) ->
    Ctx = #{<<"x">> => 1},
    ?assertEqual(Ctx, emqx_agent_pipeline_ctx:write(<<"plain">>, <<"val">>, Ctx)).

t_delete_top_level_key(_Config) ->
    Result = emqx_agent_pipeline_ctx:delete(<<"$.key">>, #{<<"key">> => 1, <<"other">> => 2}),
    ?assertEqual(#{<<"other">> => 2}, Result).

t_delete_nested_key(_Config) ->
    Ctx = #{<<"foo">> => #{<<"bar">> => 1, <<"baz">> => 2}},
    Result = emqx_agent_pipeline_ctx:delete(<<"$.foo.bar">>, Ctx),
    ?assertEqual(#{<<"foo">> => #{<<"baz">> => 2}}, Result).

t_delete_missing_path_is_noop(_Config) ->
    Ctx = #{<<"key">> => 1},
    ?assertEqual(Ctx, emqx_agent_pipeline_ctx:delete(<<"$.missing">>, Ctx)).

t_resolve_map_simple(_Config) ->
    Ctx = #{<<"event">> => #{<<"id">> => <<"x">>}},
    Map = #{<<"a">> => <<"$.event.id">>, <<"b">> => 42},
    ?assertEqual(
        #{<<"a">> => <<"x">>, <<"b">> => 42}, emqx_agent_pipeline_ctx:resolve_map(Map, Ctx)
    ).

t_resolve_map_recursive(_Config) ->
    Ctx = #{<<"key">> => <<"val">>},
    Map = #{<<"outer">> => #{<<"inner">> => <<"$.key">>}},
    ?assertEqual(
        #{<<"outer">> => #{<<"inner">> => <<"val">>}}, emqx_agent_pipeline_ctx:resolve_map(Map, Ctx)
    ).

t_resolve_map_non_map_passthrough(_Config) ->
    ?assertEqual(<<"not a map">>, emqx_agent_pipeline_ctx:resolve_map(<<"not a map">>, #{})).

t_init_creates_context(_Config) ->
    Ctx = emqx_agent_pipeline_ctx:init(#{<<"topic">> => <<"t">>}, <<"my-key">>),
    ?assertEqual(<<"my-key">>, maps:get(<<"key">>, Ctx)),
    ?assertEqual(#{<<"topic">> => <<"t">>}, maps:get(<<"event">>, Ctx)),
    ?assert(is_binary(maps:get(<<"key_base62">>, Ctx))).

t_deep_get_empty_path(_Config) ->
    ?assertEqual(42, emqx_agent_pipeline_ctx:deep_get([], 42)).

t_deep_get_nested(_Config) ->
    ?assertEqual(
        <<"v">>,
        emqx_agent_pipeline_ctx:deep_get([<<"a">>, <<"b">>], #{<<"a">> => #{<<"b">> => <<"v">>}})
    ).

t_deep_get_missing_returns_null(_Config) ->
    ?assertEqual(null, emqx_agent_pipeline_ctx:deep_get([<<"x">>], #{})).

t_deep_put_nested(_Config) ->
    Result = emqx_agent_pipeline_ctx:deep_put([<<"a">>, <<"b">>], <<"v">>, #{}),
    ?assertEqual(#{<<"a">> => #{<<"b">> => <<"v">>}}, Result).

t_deep_delete_nested(_Config) ->
    Ctx = #{<<"a">> => #{<<"b">> => 1, <<"c">> => 2}},
    Result = emqx_agent_pipeline_ctx:deep_delete([<<"a">>, <<"b">>], Ctx),
    ?assertEqual(#{<<"a">> => #{<<"c">> => 2}}, Result).
