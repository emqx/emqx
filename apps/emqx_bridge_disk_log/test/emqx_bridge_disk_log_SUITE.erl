%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    snabbkaffe:stop(),
    emqx_common_test_helpers:call_janitor(),
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
    emqx_utils_maps:deep_merge(CommonConfig, Overrides).

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
    Filepath = get_filepath_from_config(TCConfig),
    maybe
        {ok, Contents} ?= file:read_file(Filepath),
        ct:pal("raw contents:\n  ~p", [Contents]),
        emqx_connector_aggreg_json_lines_test_utils:decode(Contents)
    end.

list_rotated_logs(TCConfig) ->
    Filepath = get_filepath_from_config(TCConfig),
    Wildcard = binary_to_list(iolist_to_binary([Filepath, ".*"])),
    Contents = filelib:wildcard(Wildcard),
    lists:map(fun erlang:list_to_binary/1, Contents).

%% Rotated logs sorted from newest to oldest.
zcat_rotated_logs(TCConfig) ->
    Filepath = get_filepath_from_config(TCConfig),
    Dir = filename:dirname(Filepath),
    Wildcard = binary_to_list(iolist_to_binary([Filepath, ".*"])),
    Contents0 =
        lists:map(
            fun(File) ->
                Path = filename:join(Dir, File),
                MAt = filelib:last_modified(Path),
                {ok, Content0} = file:read_file(Path),
                Content1 = zlib:gunzip(Content0),
                Content = emqx_connector_aggreg_json_lines_test_utils:decode(Content1),
                {MAt, Content}
            end,
            filelib:wildcard(Wildcard)
        ),
    Contents1 = lists:keysort(1, Contents0),
    lists:map(fun({_MAt, C}) -> C end, lists:reverse(Contents1)).

publish(Topic, Payload) ->
    Message = emqx_message:make(Topic, Payload),
    emqx:publish(Message).

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
    ?assertMatch({201, _}, create_connector_api(Config)),
    ?assertMatch(
        {201, _},
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
            #{<<"max_file_size">> => <<(integer_to_binary(MaxSize))/binary, "B">>}
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
    ?retry(500, 10, ?assertMatch([Payload2], read_current_log(Config))),
    ?assertMatch([_], list_rotated_logs(Config)),
    ?assertMatch([[Payload1]], zcat_rotated_logs(Config)),

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
    ?assertMatch([[Payload1]], zcat_rotated_logs(Config)),

    %% Any extra data should trigger a rotation
    Payload4 = <<"2">>,
    publish(RuleTopic, Payload4),
    ?retry(500, 10, ?assertMatch([Payload4], read_current_log(Config))),
    ?assertMatch([_, _], list_rotated_logs(Config)),
    ?assertMatch(
        [[Payload2, Payload3], [Payload1]],
        zcat_rotated_logs(Config),
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
        zcat_rotated_logs(Config),
        #{payload2 => Payload2, payload3 => Payload3, payload4 => Payload4}
    ),

    ok.

%% Checks that different templates are each correctly encoded as JSONs.
t_templates(Config) when is_list(Config) ->
    ConnectorName = ?config(connector_name, Config),
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
    ConnResId = emqx_connector_resource:resource_id(?CONNECTOR_TYPE_BIN, ConnectorName),
    lists:foreach(
        fun(Case) ->
            #{template := Template, expected := Expected} = Case,
            {200, _} = update_action_api(
                Config,
                #{<<"parameters">> => #{<<"template">> => Template}}
            ),
            publish(RuleTopic, GeneralPayload),
            ok = emqx_bridge_disk_log_connector:flush(ConnResId),
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
        {201, #{<<"filepath">> := Filepath}} = create_connector_api(Config),
        {201, _} = create_action_api(Config),
        ?assertMatch({200, #{<<"status">> := <<"connected">>}}, get_connector_api(Config)),
        {ok, FileInfo} = file:read_file_info(Filepath),
        %% Make file not writable nor readable
        if_root(
            make_unreadable_mock_fn(Filepath),
            fun() ->
                on_exit(fun() -> file:write_file_info(Filepath, FileInfo) end),
                ok = file:write_file_info(Filepath, FileInfo#file_info{mode = 8#000})
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
    ?assert(filelib:is_file(Filepath)),
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
