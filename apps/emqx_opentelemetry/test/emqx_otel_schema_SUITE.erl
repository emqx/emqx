%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_otel_schema_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% Backward compatibility suite for legacy metrics converter

-define(OLD_CONF_ENABLED, <<
    "\n"
    "opentelemetry\n"
    "{\n"
    "    enable = true\n"
    "}\n"
>>).

-define(OLD_CONF_DISABLED, <<
    "\n"
    "opentelemetry\n"
    "{\n"
    "    enable = false\n"
    "}\n"
>>).

-define(OLD_CONF_ENABLED_EXPORTER, <<
    "\n"
    "opentelemetry\n"
    "{\n"
    "    enable = true\n"
    "    exporter {endpoint = \"http://127.0.0.1:4317/\", interval = 5s}\n"
    "}\n"
>>).

-define(OLD_CONF_DISABLED_EXPORTER, <<
    "\n"
    "opentelemetry\n"
    "{\n"
    "    enable = false\n"
    "    exporter {endpoint = \"http://127.0.0.1:4317/\", interval = 5s}\n"
    "}\n"
>>).

-define(OLD_CONF_EXPORTER, <<
    "\n"
    "opentelemetry\n"
    "{\n"
    "    exporter {endpoint = \"http://127.0.0.1:4317/\", interval = 5s}\n"
    "}\n"
>>).

-define(OLD_CONF_EXPORTER_PARTIAL, <<
    "\n"
    "opentelemetry\n"
    "{\n"
    "    exporter {endpoint = \"http://127.0.0.1:4317/\"}\n"
    "}\n"
>>).

-define(OLD_CONF_EXPORTER_PARTIAL1, <<
    "\n"
    "opentelemetry\n"
    "{\n"
    "    exporter {interval = 3s}\n"
    "}\n"
>>).

-define(TESTS_CONF, #{
    t_old_conf_enabled => ?OLD_CONF_ENABLED,
    t_old_conf_disabled => ?OLD_CONF_DISABLED,
    t_old_conf_enabled_exporter => ?OLD_CONF_ENABLED_EXPORTER,
    t_old_conf_disabled_exporter => ?OLD_CONF_DISABLED_EXPORTER,
    t_old_conf_exporter => ?OLD_CONF_EXPORTER,
    t_old_conf_exporter_partial => ?OLD_CONF_EXPORTER_PARTIAL,
    t_old_conf_exporter_partial1 => ?OLD_CONF_EXPORTER_PARTIAL1
}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Apps = start_apps(TC, Config, maps:get(TC, ?TESTS_CONF)),
    [{suite_apps, Apps} | Config].

end_per_testcase(_TC, Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    emqx_config:delete_override_conf_files(),
    ok.

start_apps(TC, Config, OtelConf) ->
    emqx_cth_suite:start(
        [
            {emqx_conf, OtelConf},
            emqx_management,
            emqx_opentelemetry
        ],
        #{work_dir => emqx_cth_suite:work_dir(TC, Config)}
    ).

t_old_conf_enabled(_Config) ->
    OtelConf = emqx:get_config([opentelemetry]),
    ?assertMatch(
        #{metrics := #{enable := true, interval := _}, exporter := #{endpoint := _}},
        OtelConf
    ),
    ?assertNot(erlang:is_map_key(enable, OtelConf)),
    ?assertNot(erlang:is_map_key(interval, maps:get(exporter, OtelConf))).

t_old_conf_disabled(_Config) ->
    OtelConf = emqx:get_config([opentelemetry]),
    ?assertMatch(
        #{metrics := #{enable := false, interval := _}, exporter := #{endpoint := _}},
        OtelConf
    ),
    ?assertNot(erlang:is_map_key(enable, OtelConf)),
    ?assertNot(erlang:is_map_key(interval, maps:get(exporter, OtelConf))).

t_old_conf_enabled_exporter(_Config) ->
    OtelConf = emqx:get_config([opentelemetry]),
    ?assertMatch(
        #{
            metrics := #{enable := true, interval := 5000},
            exporter := #{endpoint := <<"http://127.0.0.1:4317/">>}
        },
        OtelConf
    ),
    ?assertNot(erlang:is_map_key(enable, OtelConf)),
    ?assertNot(erlang:is_map_key(interval, maps:get(exporter, OtelConf))).

t_old_conf_disabled_exporter(_Config) ->
    OtelConf = emqx:get_config([opentelemetry]),
    ?assertMatch(
        #{
            metrics := #{enable := false, interval := 5000},
            exporter := #{endpoint := <<"http://127.0.0.1:4317/">>}
        },
        OtelConf
    ),
    ?assertNot(erlang:is_map_key(enable, OtelConf)),
    ?assertNot(erlang:is_map_key(interval, maps:get(exporter, OtelConf))).

t_old_conf_exporter(_Config) ->
    OtelConf = emqx:get_config([opentelemetry]),
    ?assertMatch(
        #{
            metrics := #{enable := false, interval := 5000},
            exporter := #{endpoint := <<"http://127.0.0.1:4317/">>}
        },
        OtelConf
    ),
    ?assertNot(erlang:is_map_key(enable, OtelConf)),
    ?assertNot(erlang:is_map_key(interval, maps:get(exporter, OtelConf))).

t_old_conf_exporter_partial(_Config) ->
    OtelConf = emqx:get_config([opentelemetry]),
    ?assertMatch(
        #{
            metrics := #{enable := false, interval := _},
            exporter := #{endpoint := <<"http://127.0.0.1:4317/">>}
        },
        OtelConf
    ),
    ?assertNot(erlang:is_map_key(enable, OtelConf)),
    ?assertNot(erlang:is_map_key(interval, maps:get(exporter, OtelConf))).

t_old_conf_exporter_partial1(_Config) ->
    OtelConf = emqx:get_config([opentelemetry]),
    ?assertMatch(
        #{
            metrics := #{enable := false, interval := 3000},
            exporter := #{endpoint := _}
        },
        OtelConf
    ),
    ?assertNot(erlang:is_map_key(enable, OtelConf)),
    ?assertNot(erlang:is_map_key(interval, maps:get(exporter, OtelConf))).
