%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Asserts that every operation in the legacy `/mqtt/topic_metrics'
%% swagger spec is tagged `deprecated: true' and that the v2 routes
%% do NOT carry the deprecated tag. Pure metadata check — no HTTP
%% interaction needed.

-module(emqx_topic_metrics_deprecation_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

t_legacy_operations_are_deprecated(_Config) ->
    Schema1 = emqx_topic_metrics_api:schema("/mqtt/topic_metrics"),
    assert_deprecated(Schema1, [get, post, put]),
    Schema2 = emqx_topic_metrics_api:schema("/mqtt/topic_metrics/:topic"),
    assert_deprecated(Schema2, [get, delete]).

t_v2_operations_are_not_deprecated(_Config) ->
    Schema1 = emqx_topic_metrics2_api:schema("/mqtt/topic_metrics2"),
    assert_not_deprecated(Schema1, [get, post, delete]),
    Schema2 = emqx_topic_metrics2_api:schema("/mqtt/topic_metrics2/:name"),
    assert_not_deprecated(Schema2, [get, delete]),
    Schema3 = emqx_topic_metrics2_api:schema("/mqtt/topic_metrics2/:name/reset"),
    assert_not_deprecated(Schema3, [put]).

assert_deprecated(Schema, Methods) ->
    lists:foreach(
        fun(M) ->
            Op = maps:get(M, Schema),
            ?assertEqual(
                true,
                maps:get(deprecated, Op, false),
                lists:flatten(io_lib:format("operation ~p missing deprecated=true", [M]))
            )
        end,
        Methods
    ).

assert_not_deprecated(Schema, Methods) ->
    lists:foreach(
        fun(M) ->
            Op = maps:get(M, Schema),
            ?assertNot(maps:get(deprecated, Op, false))
        end,
        Methods
    ).
