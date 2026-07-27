%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_disk_log_SUITE).

-feature(maybe_expr, enable).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("../src/emqx_bridge_disk_log.hrl").
-include_lib("kernel/include/file.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

%% 2026-07-01T12:00:00Z
-define(JUL1_NOON_S, 1782907200).
-define(SECONDS_PER_DAY, 86400).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, local},
        {group, on_peer}
    ].

matrix_cases() ->
    lists:filter(
        fun(TestCase) ->
            maybe
                true ?= erlang:function_exported(?MODULE, TestCase, 0),
                {matrix, true} ?= proplists:lookup(matrix, ?MODULE:TestCase()),
                true
            else
                _ -> false
            end
        end,
        emqx_common_test_helpers:all(?MODULE)
    ).

%% Tests that require mocking `disk_log' may mess up mnesia...  So we run then in a
%% separate peer node to avoid poisoning the whole suite.
on_peer_cases() ->
    lists:filter(
        fun(TestCase) ->
            maybe
                true ?= erlang:function_exported(?MODULE, TestCase, 0),
                {on_peer, true} ?= proplists:lookup(on_peer, ?MODULE:TestCase()),
                true
            else
                _ -> false
            end
        end,
        emqx_common_test_helpers:all(?MODULE)
    ).

groups() ->
    All0 = emqx_common_test_helpers:all(?MODULE),
    OnPeerCases = on_peer_cases(),
    All = All0 -- (matrix_cases() ++ OnPeerCases),
    MatrixGroups = emqx_common_test_helpers:matrix_to_groups(?MODULE, matrix_cases()),
    Groups = lists:map(fun({G, _, _}) -> {group, G} end, MatrixGroups),
    [
        {on_peer, OnPeerCases},
        {local, Groups ++ All}
        | MatrixGroups
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(on_peer, Config) ->
    Config;
init_per_group(local, Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_disk_log,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(on_peer, _Config) ->
    ok;
end_per_group(local, Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    ConnectorName = atom_to_binary(TestCase),
    PrivDir = ?config(priv_dir, Config),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Filepath = iolist_to_binary(filename:join([PrivDir, ConnectorName, UniqueNum])),
    ConnectorConfig = connector_config(#{<<"filepath">> => Filepath}),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{<<"connector">> => ConnectorName}),
    on_exit(fun() -> file:del_dir_r(Filepath) end),
    on_exit(fun emqx_bridge_v2_testlib:delete_all_bridges_and_connectors/0),
    ok = snabbkaffe:start_trace(),
    ct:timetrap({seconds, 30}),
    [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, ActionName},
        {action_config, ActionConfig}
        | Config
    ].

end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    ok = snabbkaffe:stop(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

group_path(Config, Default) ->
    case emqx_common_test_helpers:group_path(Config) of
        [] -> Default;
        [_LocalOrPeer] -> Default;
        [_LocalOrPeer | Path] -> Path
    end.

connector_config(Overrides0) ->
    InnerConfigMap0 =
        #{
            <<"enable">> => true,
            <<"tags">> => [<<"bridge">>],
            <<"description">> => <<"my cool bridge">>,
            <<"filepath">> => <<"/tmp/please_override">>,
            <<"max_file_size">> => <<"1KB">>,
            <<"max_file_number">> => 2,
            <<"resource_opts">> =>
                #{
                    <<"health_check_interval">> => <<"1s">>,
                    <<"start_after_created">> => true,
                    <<"start_timeout">> => <<"5s">>
                }
        },
    InnerConfigMap = emqx_utils_maps:deep_merge(InnerConfigMap0, Overrides0),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

action_config(Overrides) ->
    CommonConfig =
        #{
            <<"enable">> => true,
            <<"connector">> => <<"please override">>,
            <<"parameters">> =>
                #{
                    <<"template">> => <<"${.}">>,
                    <<"write_mode">> => <<"sync">>
                },
            <<"resource_opts">> => #{
                <<"batch_size">> => 1,
                <<"batch_time">> => <<"0ms">>,
                <<"buffer_mode">> => <<"memory_only">>,
                <<"buffer_seg_bytes">> => <<"10MB">>,
                <<"health_check_interval">> => <<"1s">>,
                <<"inflight_window">> => 100,
                <<"max_buffer_bytes">> => <<"256MB">>,
                <<"metrics_flush_interval">> => <<"1s">>,
                <<"query_mode">> => <<"sync">>,
                <<"request_ttl">> => <<"15s">>,
                <<"resume_interval">> => <<"1s">>,
                <<"worker_pool_size">> => 1
            }
        },
    InnerConfig = emqx_utils_maps:deep_merge(CommonConfig, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE, <<"x">>, InnerConfig).

create_connector_api(TCConfig) ->
    create_connector_api(TCConfig, _Overrides = #{}).

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

get_connector_api(TCConfig) ->
    ConnectorType = ?config(connector_type, TCConfig),
    ConnectorName = ?config(connector_name, TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_connector_api(ConnectorType, ConnectorName)
    ).

disable_connector_api(TCConfig) ->
    ConnectorType = ?config(connector_type, TCConfig),
    ConnectorName = ?config(connector_name, TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:disable_connector_api(ConnectorType, ConnectorName)
    ).

enable_connector_api(TCConfig) ->
    ConnectorType = ?config(connector_type, TCConfig),
    ConnectorName = ?config(connector_name, TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:enable_connector_api(ConnectorType, ConnectorName)
    ).

create_action_api(TCConfig) ->
    create_action_api(TCConfig, _Overrides = #{}).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_kind_api(TCConfig, Overrides)
    ).

update_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:update_bridge_api(TCConfig, Overrides)
    ).

create_rule(TCConfig, RuleTopic) ->
    emqx_bridge_v2_testlib:create_rule_and_action_http(
        ?ACTION_TYPE_BIN, RuleTopic, TCConfig, #{}
    ).

get_filepath_from_config(TCConfig) ->
    ConnectorName = ?config(connector_name, TCConfig),
    emqx_config:get_raw(
        [<<"connectors">>, ?CONNECTOR_TYPE_BIN, ConnectorName, <<"filepath">>]
    ).

read_current_log(TCConfig) ->
    Current = get_current_wrap_log(TCConfig),
    maybe
        {ok, Contents} ?= file:read_file(Current),
        ct:pal("raw contents:\n  ~p", [Contents]),
        emqx_connector_aggreg_json_lines_test_utils:decode(Contents)
    end.

get_current_wrap_log(TCConfig) ->
    Filepath = get_filepath_from_config(TCConfig),
    ConnResId = connector_resource_id(TCConfig),
    LogInfo = disk_log:info(ConnResId),
    [Current | _] = emqx_bridge_disk_log_connector:get_wrap_logs(Filepath, LogInfo),
    Current.

list_rotated_logs(TCConfig) ->
    Filepath = get_filepath_from_config(TCConfig),
    Wildcard = binary_to_list(iolist_to_binary([Filepath, ".*"])),
    Contents0 = filelib:wildcard(Wildcard),
    Contents1 = filter_wrap_logs(Contents0),
    Contents = Contents1 -- [get_current_wrap_log(TCConfig)],
    lists:map(fun erlang:list_to_binary/1, Contents).

filter_wrap_logs(Files) ->
    lists:filter(
        fun(File) ->
            not lists:suffix(".siz", File) and not lists:suffix(".idx", File)
        end,
        Files
    ).

%% Rotated logs sorted from newest to oldest.
read_rotated_logs(TCConfig) ->
    Filepath = get_filepath_from_config(TCConfig),
    Dir = filename:dirname(Filepath),
    Contents0 =
        lists:map(
            fun(File) ->
                Path = filename:join(Dir, File),
                MAt = filelib:last_modified(Path),
                {ok, Content0} = file:read_file(Path),
                Content = emqx_connector_aggreg_json_lines_test_utils:decode(Content0),
                {MAt, Content}
            end,
            list_rotated_logs(TCConfig)
        ),
    Contents1 = lists:keysort(1, Contents0),
    lists:map(fun({_MAt, C}) -> C end, lists:reverse(Contents1)).

connector_resource_id(TCConfig) ->
    emqx_bridge_v2_testlib:connector_resource_id(TCConfig).

publish(Topic, Payload) ->
    Message = emqx_message:make(Topic, Payload),
    emqx:publish(Message).

publish_and_flush(TCConfig, Topic, Payload) ->
    {_, {ok, _}} =
        ?wait_async_action(
            publish(Topic, Payload),
            #{?snk_kind := "disk_log_connector_wrote_terms"}
        ),
    ConnResId = connector_resource_id(TCConfig),
    ok = emqx_bridge_disk_log_connector:flush(ConnResId),
    ok.

%% Fixes the connector's notion of "now" so tests can deterministically cross period
%% boundaries.
set_now_s(TimeS) ->
    Key = {emqx_bridge_disk_log_connector, now_s},
    persistent_term:put(Key, TimeS),
    on_exit(fun() -> persistent_term:erase(Key) end),
    ok.

%% The base filepath the underlying `disk_log' is currently open with (date-stamped when
%% time-based rotation is active).
get_active_filepath(TCConfig) ->
    ConnResId = connector_resource_id(TCConfig),
    LogInfo = disk_log:info(ConnResId),
    {file, File} = lists:keyfind(file, 1, LogInfo),
    unicode:characters_to_binary(File).

%% Like `read_current_log/1', but based on the currently open (date-stamped) filepath
%% instead of the configured one.
read_active_log(TCConfig) ->
    ConnResId = connector_resource_id(TCConfig),
    LogInfo = disk_log:info(ConnResId),
    ActiveFilepath = get_active_filepath(TCConfig),
    [Current | _] = emqx_bridge_disk_log_connector:get_wrap_logs(ActiveFilepath, LogInfo),
    {ok, Contents} = file:read_file(Current),
    emqx_connector_aggreg_json_lines_test_utils:decode(Contents).

%% All files (including `.idx' / `.siz' bookkeeping files) belonging to the given
%% period's date stamp.
list_files_for_stamp(TCConfig, Stamp) ->
    Base = get_filepath_from_config(TCConfig),
    Wildcard = binary_to_list(<<Base/binary, "-", Stamp/binary, ".*">>),
    lists:sort(filelib:wildcard(Wildcard)).

%% All entries across all periods' file sets, in no particular order.
read_all_period_logs(TCConfig) ->
    Base = get_filepath_from_config(TCConfig),
    Wildcard = binary_to_list(<<Base/binary, "-*">>),
    Files = filter_wrap_logs(filelib:wildcard(Wildcard)),
    lists:flatmap(
        fun(File) ->
            {ok, Contents} = file:read_file(File),
            emqx_connector_aggreg_json_lines_test_utils:decode(Contents)
        end,
        Files
    ).

%% Since we run test suites in CI as `root', it's though to create a file/dir which cannot
%% be read by the current user...
if_root(YesFun, NoFun) ->
    User = string:trim(os:cmd("whoami")),
    case User == "root" of
        true ->
            _ = YesFun(),
            ok;
        false ->
            _ = NoFun(),
            ok
    end.

bin(B) when is_binary(B) -> B;
bin(S) when is_list(S) -> list_to_binary(S).

make_unreadable_mock_fn(Filepath) ->
    fun() ->
        on_exit(fun() -> meck:unload() end),
        ok = meck:new(file, [passthrough, unstick]),
        ok = meck:expect(file, read_file_info, fun(Path) ->
            case bin(Path) == bin(Filepath) of
                true ->
                    {ok, Res} = meck:passthrough([Path]),
                    {ok, Res#file_info{access = none, mode = 8#000}};
                false ->
                    meck:passthrough([Path])
            end
        end)
    end.

%% Tests that require mocking `disk_log' may mess up mnesia...  So we run then in a
%% separate peer node to avoid poisoning the whole suite.
start_peer(TestCase, TCConfig) ->
    AppSpecs = [
        emqx,
        emqx_conf,
        emqx_bridge_disk_log,
        emqx_bridge,
        emqx_rule_engine,
        emqx_management,
        emqx_mgmt_api_test_util:emqx_dashboard()
    ],
    ClusterSpec = [{peer_name(TestCase, 1), #{apps => AppSpecs}}],
    [Node] = emqx_cth_cluster:start(
        ClusterSpec,
        #{work_dir => emqx_cth_suite:work_dir(TestCase, TCConfig)}
    ),
    on_exit(fun() -> ok = emqx_cth_cluster:stop([Node]) end),
    Fun = fun() -> ?ON(Node, emqx_mgmt_api_test_util:auth_header_()) end,
    emqx_bridge_v2_testlib:set_auth_header_getter(Fun),
    Node.

peer_name(TestCase, N) ->
    binary_to_atom(
        iolist_to_binary(io_lib:format("~s_~s_~b", [?MODULE, TestCase, N]))
    ).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(Config) ->
    ok = emqx_bridge_v2_testlib:t_start_stop(Config, "disk_log_connector_stop"),
    ok.

t_create_via_http(Config) ->
    ok = emqx_bridge_v2_testlib:t_create_via_http(Config),
    ok.

t_on_get_status(Config) ->
    ok = emqx_bridge_v2_testlib:t_on_get_status(Config),
    ok.

%% Simple smoke happy path test for disk log.
t_smoke() ->
    [{matrix, true}].
t_smoke(matrix) ->
    [[sync], [async]];
t_smoke(Config) when is_list(Config) ->
    [WriteMode] = group_path(Config, [sync]),
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_connector_api(Config)
    ),
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_action_api(
            Config,
            #{<<"parameters">> => #{<<"write_mode">> => atom_to_binary(WriteMode)}}
        )
    ),
    RuleTopic = <<"smoke/t">>,
    {ok, _} = create_rule(Config, RuleTopic),
    Messages = [
        emqx_message:make(<<"c1">>, RuleTopic, <<"1">>),
        emqx_message:make(<<"c2">>, RuleTopic, <<"2">>)
    ],
    lists:foreach(fun emqx:publish/1, Messages),
    %% `disk_log' flushes to disk asynchronously, even using `blog/2'.
    ?retry(
        500,
        10,
        ?assertMatch(
            [
                #{
                    <<"client_attrs">> := #{},
                    <<"clientid">> := <<"c1">>,
                    <<"event">> := <<"message.publish">>,
                    <<"flags">> := #{},
                    <<"id">> := _,
                    <<"metadata">> := #{<<"rule_id">> := _},
                    <<"node">> := _,
                    <<"payload">> := <<"1">>,
                    <<"peerhost">> := _,
                    <<"peername">> := _,
                    <<"pub_props">> := #{},
                    <<"publish_received_at">> := _,
                    <<"qos">> := _,
                    <<"timestamp">> := _,
                    <<"topic">> := RuleTopic,
                    <<"username">> := _
                },
                #{
                    <<"clientid">> := <<"c2">>,
                    <<"payload">> := <<"2">>,
                    <<"topic">> := RuleTopic
                }
            ],
            read_current_log(Config)
        )
    ),
    %% No rotated logs yet.
    ?assertMatch([], list_rotated_logs(Config)),
    ok.

t_rotation(Config) when is_list(Config) ->
    MaxSize = 10,
    ?assertMatch(
        {201, _},
        create_connector_api(
            Config,
            #{
                <<"max_file_size">> => <<(integer_to_binary(MaxSize))/binary, "B">>,
                <<"max_file_number">> => 3
            }
        )
    ),
    ?assertMatch(
        {201, _},
        create_action_api(
            Config,
            #{<<"parameters">> => #{<<"template">> => <<"${.payload}">>}}
        )
    ),
    RuleTopic = <<"rotation">>,
    {ok, _} = create_rule(Config, RuleTopic),
    %% No data at first
    ?assertMatch([], read_current_log(Config)),
    ?assertMatch([], list_rotated_logs(Config)),
    %% At least one entry is written per log file, even it exceeds maximum size.
    Payload1 = binary:copy(<<"a">>, 100),
    publish(RuleTopic, Payload1),
    %% We see `Payload1' here because it's already decoded from JSON.
    ?retry(500, 10, ?assertMatch([Payload1], read_current_log(Config))),
    %% Log will be rotate once we try to write the next term.
    ?assertMatch([], list_rotated_logs(Config)),

    %% Now, it should trigger a rotation
    Payload2 = <<"b">>,
    publish(RuleTopic, Payload2),
    ?retry(
        500,
        10,
        ?assertMatch(
            [Payload2],
            read_current_log(Config),
            #{payload2 => Payload2}
        )
    ),
    ?assertMatch([_], list_rotated_logs(Config)),
    ?assertMatch([[Payload1]], read_rotated_logs(Config)),

    %% Current log only has `"b"\n' (4 bytes).
    CurrentBytes = 4,
    %% Shouldn't trigger rotation yet; minus one is to account for appended newline.
    %% Note: `#message.payload' won't carry an integer such as this normally.  This is to
    %% have slightly easier control over the payload size.  An integer may make its way
    %% into the template by crafting a rule + template such that the template reads an
    %% integer value from the rule output.
    Payload3 = binary_to_integer(binary:copy(<<"1">>, MaxSize - CurrentBytes - 1)),
    publish(RuleTopic, Payload3),
    ?retry(500, 10, ?assertMatch([Payload2, Payload3], read_current_log(Config))),
    ?assertMatch([_], list_rotated_logs(Config)),
    ?assertMatch([[Payload1]], read_rotated_logs(Config)),

    %% Any extra data should trigger a rotation
    Payload4 = <<"2">>,
    publish(RuleTopic, Payload4),
    ?retry(500, 10, ?assertMatch([Payload4], read_current_log(Config))),
    ?assertMatch([_, _], list_rotated_logs(Config)),
    ?assertMatch(
        [[Payload2, Payload3], [Payload1]],
        read_rotated_logs(Config),
        #{payload2 => Payload2, payload3 => Payload3}
    ),

    %% Yet another rotation should overwrite the oldest archived file.
    Payload5 = binary:copy(<<"z">>, MaxSize),
    publish(RuleTopic, Payload5),
    ?retry(500, 10, ?assertMatch([Payload5], read_current_log(Config))),
    %% No more extra log files
    ?assertMatch([_, _], list_rotated_logs(Config)),
    ?assertMatch(
        [[Payload4], [Payload2, Payload3]],
        read_rotated_logs(Config),
        #{payload2 => Payload2, payload3 => Payload3, payload4 => Payload4}
    ),

    ok.

%% Checks that different templates are each correctly encoded as JSONs.
t_templates(Config) when is_list(Config) ->
    {201, _} = create_connector_api(Config),
    {201, _} = create_action_api(Config),
    RuleTopic = <<"templates">>,
    {ok, _} = create_rule(Config, RuleTopic),
    GeneralPayload = #{
        <<"int">> => 1,
        <<"float">> => 1.23,
        <<"binary">> => <<"hey">>,
        <<"string">> => "better not to use strings in values, but...",
        <<"undefined">> => undefined,
        <<"null">> => null,
        <<"array">> => [1, <<"a">>, true, false, null, undefined, #{<<"some">> => [<<"map">>]}],
        <<"map">> => #{
            <<"nested">> => map,
            <<"complex">> => <<"structure">>,
            <<"undefined">> => undefined,
            <<"null">> => null
        }
    },
    UndefinedHint = <<
        "We have `\"undefined\"` here because it's nested"
        " inside a more complex structure."
    >>,
    Cases = [
        #{
            template => <<"${.payload.int}">>,
            expected => 1
        },
        #{
            template => <<"${.payload.float}">>,
            expected => 1.23
        },
        #{
            template => <<"${.payload.binary}">>,
            expected => <<"hey">>
        },
        #{
            template => <<"${.payload.string}">>,
            expected => "better not to use strings in values, but..."
        },
        #{
            template => <<"${.payload.undefined}">>,
            expected => null
        },
        #{
            template => <<"${.payload.null}">>,
            expected => null
        },
        #{
            template => <<"${.payload.array}">>,
            expected => [
                1, <<"a">>, true, false, null, <<"undefined">>, #{<<"some">> => [<<"map">>]}
            ],
            hint => UndefinedHint
        },
        #{
            template => <<"${.payload.map}">>,
            hint => UndefinedHint,
            expected => #{
                <<"nested">> => <<"map">>,
                <<"complex">> => <<"structure">>,
                <<"undefined">> => <<"undefined">>,
                <<"null">> => null
            }
        }
    ],
    lists:foreach(
        fun(Case) ->
            #{template := Template, expected := Expected} = Case,
            {200, _} = update_action_api(
                Config,
                #{<<"parameters">> => #{<<"template">> => Template}}
            ),
            publish_and_flush(Config, RuleTopic, GeneralPayload),
            ?retry(
                500,
                10,
                ?assertMatch(
                    Expected,
                    lists:last(read_current_log(Config)),
                    Case
                )
            ),
            ok
        end,
        Cases
    ),
    ok.

%% Connector is disconnected if the furnished filepath does not have read and write
%% permissions for the EMQX application user.
t_filepath_wrong_permissions() ->
    [{on_peer, true}].
t_filepath_wrong_permissions(Config) when is_list(Config) ->
    N = start_peer(?FUNCTION_NAME, Config),
    ?ON(N, begin
        {201, _} = create_connector_api(Config),
        {201, _} = create_action_api(Config),
        ?assertMatch({200, #{<<"status">> := <<"connected">>}}, get_connector_api(Config)),
        Current = get_current_wrap_log(Config),
        {ok, FileInfo} = file:read_file_info(Current),
        %% Make file not writable nor readable
        if_root(
            make_unreadable_mock_fn(Current),
            fun() ->
                on_exit(fun() -> file:write_file_info(Current, FileInfo) end),
                ok = file:write_file_info(Current, FileInfo#file_info{mode = 8#000})
            end
        ),
        ?retry(
            700,
            5,
            ?assertMatch(
                {200, #{<<"status">> := <<"disconnected">>}},
                get_connector_api(Config)
            )
        ),
        ok
    end),
    ok.

t_filepath_parent_not_writable() ->
    [{on_peer, true}].
t_filepath_parent_not_writable(Config) when is_list(Config) ->
    N = start_peer(?FUNCTION_NAME, Config),
    ?ON(N, begin
        #{<<"filepath">> := Filepath} = ?config(connector_config, Config),
        ParentDir = filename:dirname(Filepath),
        ok = filelib:ensure_path(ParentDir),
        {ok, ParentDirInfo} = file:read_file_info(ParentDir),
        %% Make dir not writable nor readable
        if_root(
            fun() ->
                on_exit(fun() -> meck:unload() end),
                ok = meck:new(disk_log, [passthrough, unstick, no_link]),
                ok = meck:expect(disk_log, open, fun(ArgL) ->
                    maybe
                        {file, F} ?= lists:keyfind(file, 1, ArgL),
                        true ?= bin(F) == bin(Filepath),
                        {error, eacces}
                    else
                        _ -> meck:passthrough([ArgL])
                    end
                end)
            end,
            fun() ->
                on_exit(fun() -> file:write_file_info(ParentDir, ParentDirInfo) end),
                ok = file:write_file_info(ParentDir, ParentDirInfo#file_info{mode = 8#000})
            end
        ),
        ?assertMatch(
            {201, #{
                <<"filepath">> := Filepath,
                <<"status">> := <<"disconnected">>,
                <<"status_reason">> := <<"Permission denied">>
            }},
            create_connector_api(Config)
        ),
        ok
    end),
    ok.

%% Smoke happy path test for loggin a batch of records.
t_smoke_batch() ->
    [{matrix, true}].
t_smoke_batch(matrix) ->
    [[sync], [async]];
t_smoke_batch(Config) when is_list(Config) ->
    [WriteMode] = group_path(Config, [sync]),
    BatchSize = 5,
    ?assertMatch(
        {201, _},
        create_connector_api(
            Config,
            #{<<"max_file_size">> => <<"1MB">>}
        )
    ),
    ?assertMatch(
        {201, _},
        create_action_api(
            Config,
            #{
                <<"parameters">> => #{<<"write_mode">> => atom_to_binary(WriteMode)},
                <<"resource_opts">> => #{
                    <<"batch_size">> => BatchSize,
                    <<"batch_time">> => <<"100ms">>
                }
            }
        )
    ),
    RuleTopic = <<"smoke/t">>,
    {ok, _} = create_rule(Config, RuleTopic),
    Messages = lists:map(
        fun(N) ->
            NBin = integer_to_binary(N),
            emqx_message:make(<<"c", NBin/binary>>, RuleTopic, NBin)
        end,
        lists:seq(1, BatchSize)
    ),
    ?check_trace(
        begin
            emqx_utils:pforeach(fun emqx:publish/1, Messages),
            ExpectedPayloads = lists:sort(lists:map(fun emqx_message:payload/1, Messages)),
            ?retry(
                500,
                10,
                ?assertEqual(
                    ExpectedPayloads,
                    lists:sort(
                        lists:map(fun(#{<<"payload">> := P}) -> P end, read_current_log(Config))
                    )
                )
            ),
            ?assertMatch([], list_rotated_logs(Config)),
            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [_],
                ?of_kind(call_batch_query, Trace)
            ),
            ok
        end
    ),
    ok.

%% Checks that we're able to use disk log paths with unicode characters in it.
t_unicode_paths(Config) ->
    #{<<"filepath">> := Filepath0} = ?config(connector_config, Config),
    Filepath = filename:join(Filepath0, <<"àéïõç🙈哈哈"/utf8>>),
    ?assertMatch(
        {201, #{
            <<"filepath">> := Filepath,
            <<"status">> := <<"connected">>
        }},
        create_connector_api(Config, #{<<"filepath">> => Filepath}),
        #{filepath => Filepath}
    ),
    ?assert(filelib:is_file(get_current_wrap_log(Config))),
    ok.

%% We must not allow the same filepath to be used in multiple disk log connectors.
t_duplicated_filepaths(Config) ->
    ?assertMatch({201, _}, create_connector_api(Config)),
    ?assertMatch(
        {400, #{
            <<"message">> := #{
                <<"kind">> := <<"validation_error">>,
                <<"reason">> :=
                    <<
                        "disk_log connectors must not use the same filepath; "
                        "connectors with duplicate filepaths: dup,t_duplicated_filepaths"
                    >>
            }
        }},
        create_connector_api([{connector_name, <<"dup">>} | Config])
    ),
    ok.

%% Checks that the same log file can be reopened.
t_reopen_log(Config) ->
    {201, #{<<"status">> := <<"connected">>}} =
        create_connector_api(
            Config,
            #{
                <<"max_file_size">> => <<"100B">>,
                <<"max_file_number">> => 2
            }
        ),
    {201, #{<<"status">> := <<"connected">>}} =
        create_action_api(
            Config,
            #{<<"parameters">> => #{<<"template">> => <<"${.payload}">>}}
        ),
    RuleTopic = <<"reopen">>,
    {ok, _} = create_rule(Config, RuleTopic),

    %% Write some initial data; should not rotate yet.
    Payload1 = binary:copy(<<"a">>, 50),
    publish_and_flush(Config, RuleTopic, Payload1),
    ?retry(500, 10, ?assertMatch([Payload1], read_current_log(Config))),
    ?assertMatch([], list_rotated_logs(Config)),

    %% Now we re-open the log by restarting the connector.  Contents should be unaltered
    %% (save for a potentially new extra newline)
    {204, _} = disable_connector_api(Config),
    {204, _} = enable_connector_api(Config),
    ?assertMatch([Payload1], read_current_log(Config)),
    ?assertMatch([], list_rotated_logs(Config)),
    %% A small new term shouldn't prompt a rotation either.
    Payload2 = binary:copy(<<"b">>, 30),
    publish_and_flush(Config, RuleTopic, Payload2),
    ?retry(500, 10, ?assertMatch([Payload1, Payload2], read_current_log(Config))),
    ?assertMatch([], list_rotated_logs(Config)),
    %% Now it should trigger a rotation.
    Payload3 = binary:copy(<<"c">>, 50),
    publish_and_flush(Config, RuleTopic, Payload3),
    ?retry(500, 10, ?assertMatch([Payload3], read_current_log(Config))),
    ?assertMatch([_], list_rotated_logs(Config)),
    ?assertMatch([[Payload1, Payload2]], read_rotated_logs(Config)),

    %% Reopen again.
    {204, _} = disable_connector_api(Config),
    {204, _} = enable_connector_api(Config),
    ?assertMatch([Payload3], read_current_log(Config)),
    ?assertMatch([[Payload1, Payload2]], read_rotated_logs(Config)),
    %% Provoke another rotation; should wrap to the first file.
    Payload4 = binary:copy(<<"d">>, 100),
    publish_and_flush(Config, RuleTopic, Payload4),
    ?retry(500, 10, ?assertMatch([Payload4], read_current_log(Config))),
    ?assertMatch([_], list_rotated_logs(Config)),
    ?assertMatch([[Payload3]], read_rotated_logs(Config)),

    {204, _} = disable_connector_api(Config),
    {204, _} = enable_connector_api(Config),
    ?assertMatch({200, #{<<"status">> := <<"connected">>}}, get_connector_api(Config)),

    ok.

t_rule_test_trace(Config) ->
    Opts = #{},
    emqx_bridge_v2_testlib:t_rule_test_trace(Config, Opts).

-doc """
Daily rotation: the connector writes to a date-stamped file, and switches to the next
day's file (keeping the previous day's files) once the day boundary is crossed.
""".
t_period_rotation_day(Config) ->
    set_now_s(?JUL1_NOON_S),
    {201, #{<<"status">> := <<"connected">>}} =
        create_connector_api(Config, #{<<"rotation">> => #{<<"period">> => <<"day">>}}),
    {201, _} =
        create_action_api(
            Config,
            #{<<"parameters">> => #{<<"template">> => <<"${.payload}">>}}
        ),
    RuleTopic = <<"period/day">>,
    {ok, _} = create_rule(Config, RuleTopic),
    Base = get_filepath_from_config(Config),
    ?assertEqual(<<Base/binary, "-2026070100">>, get_active_filepath(Config)),
    publish_and_flush(Config, RuleTopic, <<"before">>),
    ?retry(500, 10, ?assertMatch([<<"before">>], read_active_log(Config))),
    %% Cross the day boundary; the health check should rotate to the new day's file.
    set_now_s(?JUL1_NOON_S + ?SECONDS_PER_DAY),
    ?retry(
        500,
        20,
        ?assertEqual(<<Base/binary, "-2026070200">>, get_active_filepath(Config))
    ),
    ?assertMatch({200, #{<<"status">> := <<"connected">>}}, get_connector_api(Config)),
    %% New writes land in the new day's file.
    publish_and_flush(Config, RuleTopic, <<"after">>),
    ?retry(500, 10, ?assertMatch([<<"after">>], read_active_log(Config))),
    %% Previous day's files are left alone (no retention configured).
    ?assertMatch([_ | _], list_files_for_stamp(Config, <<"2026070100">>)),
    ok.

-doc """
Hourly rotation: the date stamp has hour granularity and rotation happens at hour
boundaries.
""".
t_period_rotation_hour(Config) ->
    set_now_s(?JUL1_NOON_S),
    {201, #{<<"status">> := <<"connected">>}} =
        create_connector_api(Config, #{<<"rotation">> => #{<<"period">> => <<"hour">>}}),
    Base = get_filepath_from_config(Config),
    ?assertEqual(<<Base/binary, "-2026070112">>, get_active_filepath(Config)),
    set_now_s(?JUL1_NOON_S + 3600),
    ?retry(
        500,
        20,
        ?assertEqual(<<Base/binary, "-2026070113">>, get_active_filepath(Config))
    ),
    ?assertMatch([_ | _], list_files_for_stamp(Config, <<"2026070112">>)),
    ?assertMatch({200, #{<<"status">> := <<"connected">>}}, get_connector_api(Config)),
    ok.

-doc """
Entries written concurrently (in both `sync` and `async` write modes) while a period
rotation happens are not lost: writes hitting the close/reopen window are retried by
the buffer worker, and every published payload ends up in one of the period's file
sets.
""".
t_period_rotation_concurrent_writes() ->
    [{matrix, true}].
t_period_rotation_concurrent_writes(matrix) ->
    [[sync], [async]];
t_period_rotation_concurrent_writes(Config) when is_list(Config) ->
    [WriteMode] = group_path(Config, [sync]),
    set_now_s(?JUL1_NOON_S),
    {201, #{<<"status">> := <<"connected">>}} =
        create_connector_api(Config, #{<<"rotation">> => #{<<"period">> => <<"day">>}}),
    {201, _} =
        create_action_api(
            Config,
            #{
                <<"parameters">> => #{
                    <<"template">> => <<"${.payload}">>,
                    <<"write_mode">> => atom_to_binary(WriteMode)
                }
            }
        ),
    RuleTopic = <<"period/concurrent">>,
    {ok, _} = create_rule(Config, RuleTopic),
    Base = get_filepath_from_config(Config),
    ?assertEqual(<<Base/binary, "-2026070100">>, get_active_filepath(Config)),
    %% Publish a continuous stream of messages while the day boundary is crossed.
    NumMessages = 200,
    TestPid = self(),
    _Publisher = spawn_link(fun() ->
        lists:foreach(
            fun(N) ->
                publish(RuleTopic, <<"m-", (integer_to_binary(N))/binary>>),
                timer:sleep(10)
            end,
            lists:seq(1, NumMessages)
        ),
        TestPid ! publisher_done
    end),
    set_now_s(?JUL1_NOON_S + ?SECONDS_PER_DAY),
    ?retry(
        500,
        20,
        ?assertEqual(<<Base/binary, "-2026070200">>, get_active_filepath(Config))
    ),
    receive
        publisher_done -> ok
    after 20_000 -> ct:fail("publisher did not finish")
    end,
    ?assertMatch({200, #{<<"status">> := <<"connected">>}}, get_connector_api(Config)),
    %% Every single published payload is found in one of the two periods' file sets.
    Expected = lists:sort([
        <<"m-", (integer_to_binary(N))/binary>>
     || N <- lists:seq(1, NumMessages)
    ]),
    ConnResId = connector_resource_id(Config),
    ?retry(
        500,
        20,
        begin
            ok = emqx_bridge_disk_log_connector:flush(ConnResId),
            ?assertEqual(Expected, lists:sort(read_all_period_logs(Config)))
        end
    ),
    ok.

-doc """
A fixed timezone offset shifts the period boundary: at 23:00 UTC with a `+02:00`
timezone, the active file is already stamped with the next day's date.
""".
t_period_rotation_timezone(Config) ->
    %% 2026-07-01T23:00:00Z == 2026-07-02T01:00:00+02:00
    set_now_s(?JUL1_NOON_S + 11 * 3600),
    {201, #{<<"status">> := <<"connected">>}} =
        create_connector_api(
            Config,
            #{
                <<"rotation">> => #{
                    <<"period">> => <<"day">>,
                    <<"timezone">> => <<"+02:00">>
                }
            }
        ),
    Base = get_filepath_from_config(Config),
    ?assertEqual(<<Base/binary, "-2026070200">>, get_active_filepath(Config)),
    ok.

-doc """
An invalid timezone is rejected at config validation time.
""".
t_period_bad_timezone(Config) ->
    ?assertMatch(
        {400, _},
        create_connector_api(
            Config,
            #{
                <<"rotation">> => #{
                    <<"period">> => <<"day">>,
                    <<"timezone">> => <<"not-a-timezone">>
                }
            }
        )
    ),
    ok.

-doc """
Retention: after a period rotation, files (including `.idx` / `.siz` bookkeeping files)
from periods older than `retention_period` are deleted, while newer ones are kept.
""".
t_period_retention(Config) ->
    set_now_s(?JUL1_NOON_S),
    {201, #{<<"status">> := <<"connected">>}} =
        create_connector_api(
            Config,
            #{
                <<"rotation">> => #{
                    <<"period">> => <<"day">>,
                    <<"retention_period">> => <<"1d">>
                }
            }
        ),
    {201, _} =
        create_action_api(
            Config,
            #{<<"parameters">> => #{<<"template">> => <<"${.payload}">>}}
        ),
    RuleTopic = <<"period/retention">>,
    {ok, _} = create_rule(Config, RuleTopic),
    Base = get_filepath_from_config(Config),
    publish_and_flush(Config, RuleTopic, <<"day1">>),
    Day1Files = list_files_for_stamp(Config, <<"2026070100">>),
    ?assertMatch([_ | _], Day1Files),
    %% `.idx' / `.siz' bookkeeping files are part of the day's file set.
    ?assert(lists:member(binary_to_list(<<Base/binary, "-2026070100.idx">>), Day1Files)),
    ?assert(lists:member(binary_to_list(<<Base/binary, "-2026070100.siz">>), Day1Files)),
    %% Next day: day 1 files are within the retention window and are kept.
    set_now_s(?JUL1_NOON_S + ?SECONDS_PER_DAY),
    ?retry(
        500,
        20,
        ?assertEqual(<<Base/binary, "-2026070200">>, get_active_filepath(Config))
    ),
    publish_and_flush(Config, RuleTopic, <<"day2">>),
    ?assertMatch([_ | _], list_files_for_stamp(Config, <<"2026070100">>)),
    %% Day after: day 1 files fall out of the retention window and are deleted.
    set_now_s(?JUL1_NOON_S + 2 * ?SECONDS_PER_DAY),
    ?retry(
        500,
        20,
        ?assertEqual(<<Base/binary, "-2026070300">>, get_active_filepath(Config))
    ),
    ?assertEqual([], list_files_for_stamp(Config, <<"2026070100">>)),
    ?assertMatch([_ | _], list_files_for_stamp(Config, <<"2026070200">>)),
    ok.

-doc """
Size-based rotation still applies within a period: exceeding `max_file_size` rotates to
the next `.N` slot of the same date-stamped file set.
""".
t_period_size_cap(Config) ->
    set_now_s(?JUL1_NOON_S),
    {201, #{<<"status">> := <<"connected">>}} =
        create_connector_api(
            Config,
            #{
                <<"max_file_size">> => <<"10B">>,
                <<"max_file_number">> => 3,
                <<"rotation">> => #{<<"period">> => <<"day">>}
            }
        ),
    {201, _} =
        create_action_api(
            Config,
            #{<<"parameters">> => #{<<"template">> => <<"${.payload}">>}}
        ),
    RuleTopic = <<"period/sizecap">>,
    {ok, _} = create_rule(Config, RuleTopic),
    Base = get_filepath_from_config(Config),
    Payload1 = binary:copy(<<"a">>, 100),
    publish_and_flush(Config, RuleTopic, Payload1),
    ?retry(500, 10, ?assertMatch([Payload1], read_active_log(Config))),
    %% Next write exceeds `max_file_size' and rotates to the next slot within the same
    %% date-stamped file set.
    Payload2 = <<"b">>,
    publish_and_flush(Config, RuleTopic, Payload2),
    ?retry(500, 10, ?assertMatch([Payload2], read_active_log(Config))),
    Day1Files = list_files_for_stamp(Config, <<"2026070100">>),
    ?assert(lists:member(binary_to_list(<<Base/binary, "-2026070100.1">>), Day1Files)),
    ?assert(lists:member(binary_to_list(<<Base/binary, "-2026070100.2">>), Day1Files)),
    ok.

-doc """
The connector notices when its log files are deleted behind its back (e.g. by an
operator or an external logrotate), reports itself disconnected, and then auto-recovers
by recreating the log files on the automatic restart.
""".
t_files_purged_externally(Config) ->
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(Config),
    {201, _} =
        create_action_api(
            Config,
            #{<<"parameters">> => #{<<"template">> => <<"${.payload}">>}}
        ),
    RuleTopic = <<"purged">>,
    {ok, _} = create_rule(Config, RuleTopic),
    publish_and_flush(Config, RuleTopic, <<"before">>),
    ?retry(500, 10, ?assertMatch([<<"before">>], read_current_log(Config))),
    %% Purge all log files (including `.idx' / `.siz') behind the connector's back.
    Base = get_filepath_from_config(Config),
    Files = filelib:wildcard(binary_to_list(<<Base/binary, ".*">>)),
    ?assertMatch([_ | _], Files),
    lists:foreach(fun(F) -> ok = file:delete(F) end, Files),
    %% The health check notices the missing files...
    ?retry(
        700,
        20,
        ?assertMatch({200, #{<<"status">> := <<"disconnected">>}}, get_connector_api(Config))
    ),
    %% ... and the automatic restart recreates them and reconnects.
    ?retry(
        700,
        20,
        ?assertMatch({200, #{<<"status">> := <<"connected">>}}, get_connector_api(Config))
    ),
    publish_and_flush(Config, RuleTopic, <<"after">>),
    ?retry(500, 10, ?assertMatch([<<"after">>], read_current_log(Config))),
    ok.

-doc """
`period = none` (the default) keeps today's behavior: the log is opened with the
configured filepath verbatim, and no date-stamped files are created.
""".
t_period_none(Config) ->
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(Config),
    {201, _} =
        create_action_api(
            Config,
            #{<<"parameters">> => #{<<"template">> => <<"${.payload}">>}}
        ),
    RuleTopic = <<"period/none">>,
    {ok, _} = create_rule(Config, RuleTopic),
    Base = get_filepath_from_config(Config),
    ?assertEqual(Base, get_active_filepath(Config)),
    publish_and_flush(Config, RuleTopic, <<"hello">>),
    ?retry(500, 10, ?assertMatch([<<"hello">>], read_current_log(Config))),
    %% Still on the configured (un-stamped) filepath after health checks.
    ?assertEqual(Base, get_active_filepath(Config)),
    ok.
