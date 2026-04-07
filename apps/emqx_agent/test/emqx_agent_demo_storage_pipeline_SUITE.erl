%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_demo_storage_pipeline_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(PIPE_EVENTS_FILTER, <<"pipe/+/inst/+/events">>).
-define(CMD_FILTER, <<"demo-storage/cmd/+">>).
-define(SHORT_TIMEOUT, 8_000).
-define(FIREWORKS_BASE_URL, <<"https://api.fireworks.ai/inference/v1">>).
-define(DEFAULT_MODEL, <<"accounts/fireworks/models/kimi-k2p5">>).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx_agent],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    ApiKey = must_env("FIREWORKS_API_KEY"),
    Model = env_or_default("FIREWORKS_MODEL", ?DEFAULT_MODEL),
    ok = ensure_fireworks_available(ApiKey, Model),
    [{suite_apps, Apps}, {fireworks_key, ApiKey}, {fireworks_model, Model} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(TestCase, Config) ->
    Id = atom_to_binary(TestCase, utf8),
    ok = ensure_tables(),
    ok = truncate_tables(),
    emqx:subscribe(?PIPE_EVENTS_FILTER),
    emqx:subscribe(?CMD_FILTER),
    [{tc_id, Id} | Config].

end_per_testcase(_TestCase, _Config) ->
    emqx_agent_pipeline_registry:delete_all(),
    emqx_agent_skill_registry:delete_all(),
    emqx:unsubscribe(?PIPE_EVENTS_FILTER),
    emqx:unsubscribe(?CMD_FILTER),
    ok.

t_field_update_sets_parked_and_occupancy(Config) ->
    Id = ?config(tc_id, Config),
    register_storage_skills(Id),

    PipelineId = <<"demo_storage_field_update_", Id/binary>>,
    TrigTopic = <<"evt/demo-storage/tele/", Id/binary>>,
    ok = emqx_agent_pipeline_registry:register(#{
        <<"pipeline_id">> => PipelineId,
        <<"trigger">> => #{<<"topic">> => TrigTopic},
        <<"steps">> => field_update_steps(Config, Id)
    }),

    insert_device(Id, <<"L">>, <<"free">>),
    Event = telemetry_event(Id, <<"L">>, true, [
        #{<<"x">> => 1, <<"y">> => 14},
        #{<<"x">> => 1, <<"y">> => 15},
        #{<<"x">> => 1, <<"y">> => 16},
        #{<<"x">> => 2, <<"y">> => 16}
    ]),
    publish_evt(TrigTopic, Event),

    ok = wait_until(fun() -> fetch_device_status(Id) =:= <<"parked">> end, 25_000),
    ok = wait_until(fun() -> length(fetch_occupied_boxes(Id)) =:= 4 end, 25_000),
    ?assertEqual([
        #{<<"x">> => 1, <<"y">> => 14},
        #{<<"x">> => 1, <<"y">> => 15},
        #{<<"x">> => 1, <<"y">> => 16},
        #{<<"x">> => 2, <<"y">> => 16}
    ], fetch_occupied_boxes(Id)).

t_field_update_noop_when_already_parked(Config) ->
    Id = ?config(tc_id, Config),
    register_storage_skills(Id),

    PipelineId = <<"demo_storage_field_update_noop_", Id/binary>>,
    TrigTopic = <<"evt/demo-storage/tele/", Id/binary>>,
    ok = emqx_agent_pipeline_registry:register(#{
        <<"pipeline_id">> => PipelineId,
        <<"trigger">> => #{<<"topic">> => TrigTopic},
        <<"steps">> => field_update_steps(Config, Id)
    }),

    insert_device(Id, <<"T">>, <<"parked">>),
    Event = telemetry_event(Id, <<"T">>, true, [
        #{<<"x">> => 5, <<"y">> => 12},
        #{<<"x">> => 4, <<"y">> => 13},
        #{<<"x">> => 5, <<"y">> => 13},
        #{<<"x">> => 6, <<"y">> => 13}
    ]),
    publish_evt(TrigTopic, Event),

    timer:sleep(500),
    ?assertEqual(<<"parked">>, fetch_device_status(Id)),
    ?assertEqual([], fetch_occupied_boxes(Id)).

t_core_pipeline_breaks_for_parked_telemetry(Config) ->
    Id = ?config(tc_id, Config),
    register_storage_skills(Id),
    CmdSkill = command_skill_id(Id),
    ok = emqx_agent_skill_publish:create(#{
        skill_id => CmdSkill,
        desc => <<"Publish command to storage device">>,
        topic_prefix => <<"demo-storage/cmd/">>,
        payload_schema => command_payload_schema()
    }),

    PipelineId = <<"demo_storage_core_break_", Id/binary>>,
    TrigTopic = <<"evt/demo-storage/tele/", Id/binary>>,
    ok = emqx_agent_pipeline_registry:register(#{
        <<"pipeline_id">> => PipelineId,
        <<"trigger">> => #{<<"topic">> => TrigTopic},
        <<"steps">> => core_steps(Config, Id, CmdSkill)
    }),

    Event = telemetry_event(Id, <<"O">>, true, [
        #{<<"x">> => 3, <<"y">> => 15},
        #{<<"x">> => 4, <<"y">> => 15},
        #{<<"x">> => 3, <<"y">> => 16},
        #{<<"x">> => 4, <<"y">> => 16}
    ]),
    publish_evt(TrigTopic, Event),

    timer:sleep(500),
    ?assertEqual(timeout, recv_command_or_timeout(Id, 1000)).

t_core_pipeline_emits_park_for_tightly_packed(Config) ->
    Id = ?config(tc_id, Config),
    register_storage_skills(Id),
    CmdSkill = command_skill_id(Id),
    ok = emqx_agent_skill_publish:create(#{
        skill_id => CmdSkill,
        desc => <<"Publish command to storage device">>,
        topic_prefix => <<"demo-storage/cmd/">>,
        payload_schema => command_payload_schema()
    }),

    PipelineId = <<"demo_storage_core_park_", Id/binary>>,
    TrigTopic = <<"evt/demo-storage/tele/", Id/binary>>,
    ok = emqx_agent_pipeline_registry:register(#{
        <<"pipeline_id">> => PipelineId,
        <<"trigger">> => #{<<"topic">> => TrigTopic},
        <<"steps">> => core_steps(Config, Id, CmdSkill)
    }),

    Event = telemetry_event(Id, <<"I">>, false, [
        #{<<"x">> => 6, <<"y">> => 17},
        #{<<"x">> => 7, <<"y">> => 17},
        #{<<"x">> => 8, <<"y">> => 17},
        #{<<"x">> => 9, <<"y">> => 17}
    ]),
    publish_evt(TrigTopic, Event),
    case recv_command_or_timeout(Id, 20_000) of
        timeout ->
            Completed = recv_pipeline_completed(PipelineId, 20_000),
            Ctx = maps:get(<<"context">>, Completed, #{}),
            Core = maps:get(<<"core_decision">>, Ctx, #{}),
            ?assertEqual(<<"park">>, core_command(Core));
        Cmd ->
            ?assertEqual(<<"park">>, maps:get(<<"command">>, Cmd))
    end.

t_full_path_two_t_devices_packed(Config) ->
    Id = ?config(tc_id, Config),
    CmdSkill = command_skill_id(Id),
    ok = emqx_agent_skill_publish:create(#{
        skill_id => CmdSkill,
        desc => <<"Publish command to storage device">>,
        topic_prefix => <<"demo-storage/cmd/">>,
        payload_schema => command_payload_schema()
    }),

    Device1 = <<Id/binary, "-d1">>,
    Device2 = <<Id/binary, "-d2">>,
    ok = insert_device(Device1, <<"T">>, <<"free">>),
    ok = insert_device(Device2, <<"T">>, <<"free">>),

    register_device_pipelines(Config, Device1, CmdSkill),
    register_device_pipelines(Config, Device2, CmdSkill),

    {Cmds1, _Final1} = run_device_start(Device1, <<"T">>, 4, 0),
    {Cmds2, _Final2} = run_device_start(Device2, <<"T">>, 9, 0),

    ?assert(has_command(Cmds1, <<"rotate">>)),
    ?assert(has_command(Cmds1, <<"park">>)),
    ?assert(has_command(Cmds2, <<"rotate">>)),
    ?assert(has_command(Cmds2, <<"park">>)),
    ?assert(has_lateral_move(Cmds2)),

    ?assertEqual(<<"parked">>, fetch_device_status(Device1)),
    ?assertEqual(<<"parked">>, fetch_device_status(Device2)),
    ?assertEqual(4, length(fetch_occupied_boxes(Device1))),
    ?assertEqual(4, length(fetch_occupied_boxes(Device2))).

register_device_pipelines(Config, DeviceId, CmdSkill) ->
    TrigTopic = <<"evt/demo-storage/tele/", DeviceId/binary>>,
    FieldPipeline = <<"demo_storage_field_update_", DeviceId/binary>>,
    CorePipeline = <<"demo_storage_core_", DeviceId/binary>>,
    ok = emqx_agent_pipeline_registry:register(#{
        <<"pipeline_id">> => FieldPipeline,
        <<"trigger">> => #{<<"topic">> => TrigTopic},
        <<"steps">> => field_update_steps(Config, DeviceId)
    }),
    ok = emqx_agent_pipeline_registry:register(#{
        <<"pipeline_id">> => CorePipeline,
        <<"trigger">> => #{<<"topic">> => TrigTopic},
        <<"steps">> => core_steps(Config, DeviceId, CmdSkill)
    }).

run_device_start(DeviceId, Shape, StartX, StartY) ->
    Topic = <<"evt/demo-storage/tele/", DeviceId/binary>>,
    Device0 = #{
        id => DeviceId,
        shape => Shape,
        cells => t_shape_cells(),
        x => StartX,
        y => StartY,
        parked => false
    },
    emit_telemetry_with_print(Device0, Topic, <<"spawn">>),
    run_device_until_parked(Device0, Topic, [], 0).

run_device_until_parked(Device, _Topic, _Cmds, N) when N > 60 ->
    ct:fail("device did not park in 60 steps: ~p", [maps:get(id, Device)]);
run_device_until_parked(#{id := DeviceId, parked := true} = Device, _Topic, Cmds, _N) ->
    ok = wait_until(fun() -> fetch_device_status(DeviceId) =:= <<"parked">> end, 30_000),
    ok = wait_until(fun() -> length(fetch_occupied_boxes(DeviceId)) =:= 4 end, 30_000),
    {lists:reverse(Cmds), Device};
run_device_until_parked(#{id := DeviceId} = Device, Topic, Cmds, N) ->
    Cmd = recv_command(DeviceId),
    Occupied = fetch_all_occupied_boxes(),
    {Device2, Reason} = apply_command(Device, Cmd, Occupied),
    emit_telemetry_with_print(Device2, Topic, Reason),
    run_device_until_parked(Device2, Topic, [Cmd | Cmds], N + 1).

apply_command(#{parked := true} = Device, _Cmd, _Occupied) ->
    {Device, <<"ignored_move_parked">>};
apply_command(Device, #{<<"command">> := <<"park">>}, _Occupied) ->
    {Device#{parked => true}, <<"park">>};
apply_command(Device, #{<<"command">> := <<"rotate">>}, Occupied) ->
    Rotated = normalize_cells([#{x => -maps:get(y, C), y => maps:get(x, C)} || C <- maps:get(cells, Device)]),
    case fits_boxes(device_boxes(Device#{cells => Rotated}), Occupied) of
        true -> {Device#{cells => Rotated}, <<"rotate">>};
        false -> {Device, <<"blocked_rotate">>}
    end;
apply_command(Device, #{<<"command">> := <<"move">>, <<"direction">> := Dir} = Cmd, Occupied) ->
    Dist = maps:get(<<"distance">>, Cmd, 1),
    move_with_distance(Device, Dir, Dist, Occupied).

move_with_distance(Device, _Dir, Dist, _Occupied) when Dist =< 0 ->
    {Device, <<"blocked">>};
move_with_distance(Device, Dir, Dist, Occupied) ->
    Delta =
        case Dir of
            <<"left">> -> {-1, 0};
            <<"right">> -> {1, 0};
            _ -> {0, 1}
        end,
    move_steps(Device, Dir, Dist, Delta, Occupied, false).

move_steps(Device, Dir, 0, _Delta, _Occupied, true) ->
    {Device, <<"move_", Dir/binary>>};
move_steps(Device, _Dir, 0, _Delta, _Occupied, false) ->
    {Device, <<"blocked">>};
move_steps(Device, Dir, Dist, {DX, DY}, Occupied, AnyMoved) ->
    Device1 = Device#{x => maps:get(x, Device) + DX, y => maps:get(y, Device) + DY},
    case fits_boxes(device_boxes(Device1), Occupied) of
        true -> move_steps(Device1, Dir, Dist - 1, {DX, DY}, Occupied, true);
        false ->
            case AnyMoved of
                true -> {Device, <<"move_", Dir/binary>>};
                false -> {Device, <<"blocked">>}
            end
    end.

emit_telemetry_with_print(Device, Topic, Reason) ->
    print_field(maps:get(id, Device), Reason, [Device]),
    Event = telemetry_event(maps:get(id, Device), maps:get(shape, Device), maps:get(parked, Device), device_boxes(Device)),
    publish_evt(Topic, Event).

print_field(DeviceId, Reason, ActiveDevices) ->
    Parked = fetch_all_occupied_boxes(),
    Active = lists:append([device_boxes(D) || D <- ActiveDevices, maps:get(parked, D) =:= false]),
    ActiveSet = maps:from_list([{{maps:get(<<"x">>, B), maps:get(<<"y">>, B)}, true} || B <- Active]),
    ParkedSet = maps:from_list([{{maps:get(<<"x">>, B), maps:get(<<"y">>, B)}, true} || B <- Parked]),
    Rows = [
        iolist_to_binary([
            cell_token(X, Y, ActiveSet, ParkedSet) || X <- lists:seq(0, 13)
        ])
     || Y <- lists:seq(0, 17)
    ],
    ct:print("~nfield telemetry device=~ts reason=~ts~n~ts", [
        DeviceId,
        Reason,
        iolist_to_binary(string:join([binary_to_list(R) || R <- Rows], "\n"))
    ]).

cell_token(X, Y, ActiveSet, ParkedSet) ->
    case maps:is_key({X, Y}, ParkedSet) of
        true -> <<"XX">>;
        false ->
            case maps:is_key({X, Y}, ActiveSet) of
                true -> <<"##">>;
                false -> <<"..">>
            end
    end.

fits_boxes(Boxes, Occupied) ->
    OccupiedSet = maps:from_list([{{maps:get(<<"x">>, B), maps:get(<<"y">>, B)}, true} || B <- Occupied]),
    lists:all(
        fun(#{<<"x">> := X, <<"y">> := Y}) ->
            X >= 0 andalso X < 14 andalso Y >= 0 andalso Y < 18 andalso not maps:is_key({X, Y}, OccupiedSet)
        end,
        Boxes
    ).

device_boxes(Device) ->
    X0 = maps:get(x, Device),
    Y0 = maps:get(y, Device),
    [
        #{<<"x">> => X0 + maps:get(x, C), <<"y">> => Y0 + maps:get(y, C)}
     || C <- maps:get(cells, Device)
    ].

t_shape_cells() ->
    [#{x => 0, y => 0}, #{x => 1, y => 0}, #{x => 2, y => 0}, #{x => 1, y => 1}].

normalize_cells(Cells) ->
    MinX = lists:min([maps:get(x, C) || C <- Cells]),
    MinY = lists:min([maps:get(y, C) || C <- Cells]),
    [#{x => maps:get(x, C) - MinX, y => maps:get(y, C) - MinY} || C <- Cells].

has_command(Cmds, Cmd) ->
    lists:any(fun(C) -> maps:get(<<"command">>, C, undefined) =:= Cmd end, Cmds).

has_lateral_move(Cmds) ->
    lists:any(
        fun(C) ->
            maps:get(<<"command">>, C, undefined) =:= <<"move">> andalso
                (maps:get(<<"direction">>, C, undefined) =:= <<"left">> orelse
                    maps:get(<<"direction">>, C, undefined) =:= <<"right">>)
        end,
        Cmds
    ).

field_update_steps(Config, Id) ->
    [
        #{
            <<"id">> => <<"load_status">>,
            <<"type">> => <<"call_skill">>,
            <<"skill">> => <<"postgresql.query@", (pg_status_skill_id(Id))/binary>>,
            <<"args">> => #{<<"device_id">> => <<"$.event.device_id">>},
            <<"result_path">> => <<"$.db_status">>
        },
        #{
            <<"id">> => <<"skip_if_not_parked">>,
            <<"type">> => <<"break">>,
            <<"path">> => <<"$.event.parked">>,
            <<"not">> => true
        },
        #{
            <<"id">> => <<"apply_parking">>,
            <<"type">> => <<"llm_loop">>,
            <<"session_config">> => session_config(Config, field_update_prompt()),
            <<"tools">> => [
                <<"postgresql.query@", (pg_set_status_skill_id(Id))/binary>>,
                <<"postgresql.query@", (pg_set_box_skill_id(Id))/binary>>
            ],
            <<"input">> => #{
                <<"event">> => <<"$.event">>,
                <<"db_status">> => <<"$.db_status">>
            },
            <<"result_path">> => <<"$.field_update_decision">>
        }
    ].

core_steps(Config, Id, CmdSkill) ->
    [
        #{
            <<"id">> => <<"stop_if_reported_parked">>,
            <<"type">> => <<"break">>,
            <<"path">> => <<"$.event.parked">>
        },
        #{
            <<"id">> => <<"load_field_map">>,
            <<"type">> => <<"call_skill">>,
            <<"skill">> => <<"postgresql.query@", (pg_field_skill_id(Id))/binary>>,
            <<"args">> => #{},
            <<"result_path">> => <<"$.field_map">>
        },
        #{
            <<"id">> => <<"choose_command">>,
            <<"type">> => <<"llm_loop">>,
            <<"session_config">> => session_config(Config, core_prompt()),
            <<"tools">> => [
                <<"message.publish@", CmdSkill/binary>>,
                <<"postgresql.query@", (pg_status_skill_id(Id))/binary>>
            ],
            <<"input">> => #{
                <<"field_width">> => 14,
                <<"field_height">> => 18,
                <<"event">> => <<"$.event">>,
                <<"field_map">> => <<"$.field_map">>
            },
            <<"result_path">> => <<"$.core_decision">>
        }
    ].

field_update_prompt() ->
    <<
        "You are a strict tool planner for storage parking updates.\n"
        "Input JSON has event and db_status.\n"
        "Rules:\n"
        "1) If event.parked is false: call no tool and return {\"action\":\"noop_not_parked\"}.\n"
        "2) If db_status.rows[0].status is parked: call no tool and return {\"action\":\"noop_already_parked\"}.\n"
        "3) Otherwise call exactly two tools in order:\n"
        "   a) tool name containing pg_set_status_ with args {\"device_id\": event.device_id}.\n"
        "   b) tool name containing pg_set_box_ with args:\n"
        "      {\"device_id\":event.device_id,\"x1\":event.boxes[0].x,\"y1\":event.boxes[0].y,\n"
        "       \"x2\":event.boxes[1].x,\"y2\":event.boxes[1].y,\n"
        "       \"x3\":event.boxes[2].x,\"y3\":event.boxes[2].y,\n"
        "       \"x4\":event.boxes[3].x,\"y4\":event.boxes[3].y}.\n"
        "4) Return JSON {\"action\":\"updated\"}.\n"
        "Do not call any other tools."
    >>.

core_prompt() ->
    <<
        "You are a strict single-step controller for storage devices.\n"
        "Goal: pack devices from the bottom with minimal free cells and no overlaps.\n"
        "Input JSON has event, field_map, field_width, field_height.\n"
        "Allowed commands: rotate, move(left|right|down,distance>=1), park.\n"
        "Rules:\n"
        "1) If event.parked is true, call no tool and return {\"command\":\"none\"}.\n"
        "2) Evaluate candidate next actions that are legal under bounds and occupancy: rotate, move left/right/down with integer distance >= 1, or park.\n"
        "3) Prefer actions that reduce final free cells (holes) and lower the stack height; bottom-compaction has higher priority than short-term movement.\n"
        "4) Choose lateral direction and distance yourself from field_map and event.boxes to improve packing quality.\n"
        "5) Use rotate when it improves reachability or reduces holes after settling.\n"
        "6) Send park only when the device is already well-packed and further legal moves do not improve compaction.\n"
        "7) Call at most one tool each step and return JSON with key command."
    >>.

command_payload_schema() ->
    #{
        <<"oneOf">> => [
            #{
                <<"type">> => <<"object">>,
                <<"properties">> => #{<<"command">> => #{<<"type">> => <<"string">>, <<"enum">> => [<<"park">>]}},
                <<"required">> => [<<"command">>]
            },
            #{
                <<"type">> => <<"object">>,
                <<"properties">> => #{<<"command">> => #{<<"type">> => <<"string">>, <<"enum">> => [<<"rotate">>]}},
                <<"required">> => [<<"command">>]
            },
            #{
                <<"type">> => <<"object">>,
                <<"properties">> => #{
                    <<"command">> => #{<<"type">> => <<"string">>, <<"enum">> => [<<"move">>]},
                    <<"direction">> => #{<<"type">> => <<"string">>, <<"enum">> => [<<"left">>, <<"right">>, <<"down">>]},
                    <<"distance">> => #{<<"type">> => <<"integer">>, <<"minimum">> => 1}
                },
                <<"required">> => [<<"command">>, <<"direction">>, <<"distance">>]
            }
        ]
    }.

session_config(Config, Instructions) ->
    #{
        <<"api_key">> => ?config(fireworks_key, Config),
        <<"base_url">> => ?FIREWORKS_BASE_URL,
        <<"model">> => ?config(fireworks_model, Config),
        <<"instructions">> => Instructions,
        <<"output_schema">> => #{
            <<"type">> => <<"object">>,
            <<"properties">> => #{
                <<"action">> => #{<<"type">> => <<"string">>},
                <<"command">> => #{<<"type">> => <<"string">>}
            }
        }
    }.

register_storage_skills(Id) ->
    ok = emqx_agent_skill_postgresql:create(#{
        skill_id => pg_status_skill_id(Id),
        desc => <<"Get device status">>,
        query => <<"SELECT status FROM demo_storage_devices WHERE device_id = $1">>,
        arg_keys => [<<"device_id">>],
        input_schema => status_get_input_schema(),
        output_schema => status_get_output_schema()
    }),
    ok = emqx_agent_skill_postgresql:create(#{
        skill_id => pg_set_status_skill_id(Id),
        desc => <<"Set device status parked">>,
        query => <<"UPDATE demo_storage_devices SET status = 'parked' WHERE device_id = $1">>,
        arg_keys => [<<"device_id">>],
        input_schema => status_set_input_schema(),
        output_schema => mutation_output_schema()
    }),
    ok = emqx_agent_skill_postgresql:create(#{
        skill_id => pg_set_box_skill_id(Id),
        desc => <<"Insert occupied boxes">>,
        query =>
            <<
                "INSERT INTO demo_storage_field_boxes(device_id, x, y) VALUES "
                "($1,$2,$3),($4,$5,$6),($7,$8,$9),($10,$11,$12) "
                "ON CONFLICT (x,y) DO NOTHING"
            >>,
        arg_keys => [
            <<"device_id">>, <<"x1">>, <<"y1">>,
            <<"device_id">>, <<"x2">>, <<"y2">>,
            <<"device_id">>, <<"x3">>, <<"y3">>,
            <<"device_id">>, <<"x4">>, <<"y4">>
        ],
        input_schema => set_boxes_input_schema(),
        output_schema => mutation_output_schema()
    }),
    ok = emqx_agent_skill_postgresql:create(#{
        skill_id => pg_field_skill_id(Id),
        desc => <<"Get field occupancy">>,
        query => <<"SELECT x, y, device_id FROM demo_storage_field_boxes ORDER BY y, x">>,
        arg_keys => [],
        input_schema => empty_object_schema(),
        output_schema => field_map_output_schema()
    }).

empty_object_schema() ->
    #{<<"type">> => <<"object">>}.

status_get_input_schema() ->
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => #{
            <<"device_id">> => #{<<"type">> => <<"string">>}
        },
        <<"required">> => [<<"device_id">>]
    }.

status_set_input_schema() -> status_get_input_schema().

status_get_output_schema() ->
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => #{
            <<"rows">> => #{
                <<"type">> => <<"array">>,
                <<"items">> => #{
                    <<"type">> => <<"object">>,
                    <<"properties">> => #{
                        <<"status">> => #{<<"type">> => <<"string">>, <<"enum">> => [<<"free">>, <<"parked">>]}
                    },
                    <<"required">> => [<<"status">>]
                }
            }
        },
        <<"required">> => [<<"rows">>]
    }.

set_boxes_input_schema() ->
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => #{
            <<"device_id">> => #{<<"type">> => <<"string">>},
            <<"x1">> => #{<<"type">> => <<"integer">>}, <<"y1">> => #{<<"type">> => <<"integer">>},
            <<"x2">> => #{<<"type">> => <<"integer">>}, <<"y2">> => #{<<"type">> => <<"integer">>},
            <<"x3">> => #{<<"type">> => <<"integer">>}, <<"y3">> => #{<<"type">> => <<"integer">>},
            <<"x4">> => #{<<"type">> => <<"integer">>}, <<"y4">> => #{<<"type">> => <<"integer">>}
        },
        <<"required">> => [
            <<"device_id">>,
            <<"x1">>, <<"y1">>,
            <<"x2">>, <<"y2">>,
            <<"x3">>, <<"y3">>,
            <<"x4">>, <<"y4">>
        ]
    }.

field_map_output_schema() ->
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => #{
            <<"rows">> => #{
                <<"type">> => <<"array">>,
                <<"items">> => #{
                    <<"type">> => <<"object">>,
                    <<"properties">> => #{
                        <<"x">> => #{<<"type">> => <<"integer">>},
                        <<"y">> => #{<<"type">> => <<"integer">>},
                        <<"device_id">> => #{<<"type">> => <<"string">>}
                    },
                    <<"required">> => [<<"x">>, <<"y">>, <<"device_id">>]
                }
            }
        },
        <<"required">> => [<<"rows">>]
    }.

mutation_output_schema() ->
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => #{
            <<"num_rows">> => #{<<"type">> => <<"integer">>}
        }
    }.

publish_evt(Topic, Event) ->
    Msg = emqx_message:make(?MODULE, 0, Topic, emqx_utils_json:encode(Event)),
    emqx_broker:publish(Msg).

telemetry_event(DeviceId, Shape, Parked, Boxes) ->
    #{
        <<"device_id">> => DeviceId,
        <<"shape">> => Shape,
        <<"status">> =>
            case Parked of
                true -> <<"parked">>;
                false -> <<"free">>
            end,
        <<"parked">> => Parked,
        <<"boxes">> => Boxes
    }.

recv_pipe_event(PipelineId) ->
    receive
        #deliver{topic = <<"pipe/", _/binary>>, message = #message{payload = P}} ->
            Frame = emqx_utils_json:decode(P),
            case maps:get(<<"pipeline_id">>, Frame, undefined) of
                PipelineId -> Frame;
                _ -> recv_pipe_event(PipelineId)
            end
    after ?SHORT_TIMEOUT ->
        ct:fail("no pipeline event for ~s", [PipelineId])
    end.

recv_pipeline_completed(PipelineId, Timeout) ->
    case recv_pipe_event_or_timeout(PipelineId, Timeout) of
        timeout ->
            ct:fail("no pipeline_completed event for ~s", [PipelineId]);
        #{<<"type">> := <<"pipeline_completed">>} = Frame ->
            Frame;
        _Other ->
            recv_pipeline_completed(PipelineId, Timeout)
    end.

recv_pipe_event_or_timeout(PipelineId, Timeout) ->
    receive
        #deliver{topic = <<"pipe/", _/binary>>, message = #message{payload = P}} ->
            Frame = emqx_utils_json:decode(P),
            case maps:get(<<"pipeline_id">>, Frame, undefined) of
                PipelineId -> Frame;
                _ -> recv_pipe_event_or_timeout(PipelineId, Timeout)
            end
    after Timeout ->
        timeout
    end.

recv_command(DeviceId) ->
    receive
        #deliver{topic = <<"demo-storage/cmd/", DeviceId/binary>>, message = #message{payload = P}} ->
            decode_json_or_fail(P)
    after ?SHORT_TIMEOUT ->
        ct:fail("no command published for ~s", [DeviceId])
    end.

recv_command_or_timeout(DeviceId, Timeout) ->
    receive
        #deliver{topic = <<"demo-storage/cmd/", DeviceId/binary>>, message = #message{payload = P}} ->
            decode_json_or_fail(P)
    after Timeout ->
        timeout
    end.

ensure_tables() ->
    ResId = emqx_agent_skill_postgresql:resource_id(),
    ok = query_ok(emqx_resource:simple_sync_query(
        ResId,
        {query,
            <<
                "CREATE TABLE IF NOT EXISTS demo_storage_devices ("
                "device_id TEXT PRIMARY KEY,"
                "shape TEXT NOT NULL,"
                "status TEXT NOT NULL)"
            >>}
    )),
    ok = query_ok(emqx_resource:simple_sync_query(
        ResId,
        {query,
            <<
                "CREATE TABLE IF NOT EXISTS demo_storage_field_boxes ("
                "x INT NOT NULL,"
                "y INT NOT NULL,"
                "device_id TEXT NOT NULL,"
                "PRIMARY KEY(x, y))"
            >>}
    )).

truncate_tables() ->
    ResId = emqx_agent_skill_postgresql:resource_id(),
    ok = query_ok(emqx_resource:simple_sync_query(ResId, {query, <<"DELETE FROM demo_storage_field_boxes">>})),
    ok = query_ok(emqx_resource:simple_sync_query(ResId, {query, <<"DELETE FROM demo_storage_devices">>})).

insert_device(DeviceId, Shape, Status) ->
    ResId = emqx_agent_skill_postgresql:resource_id(),
    ok = query_ok(emqx_resource:simple_sync_query(
        ResId,
        {query,
            <<"INSERT INTO demo_storage_devices(device_id, shape, status) VALUES ($1, $2, $3)">>,
            [DeviceId, Shape, Status]}
    )).

fetch_device_status(DeviceId) ->
    ResId = emqx_agent_skill_postgresql:resource_id(),
    {ok, _Cols, [{Status}]} = emqx_resource:simple_sync_query(
        ResId,
        {query, <<"SELECT status FROM demo_storage_devices WHERE device_id = $1">>, [DeviceId]}
    ),
    Status.

fetch_occupied_boxes(DeviceId) ->
    ResId = emqx_agent_skill_postgresql:resource_id(),
    {ok, _Cols, Rows} = emqx_resource:simple_sync_query(
        ResId,
        {query, <<"SELECT x, y FROM demo_storage_field_boxes WHERE device_id = $1 ORDER BY y, x">>, [DeviceId]}
    ),
    [#{<<"x">> => X, <<"y">> => Y} || {X, Y} <- Rows].

fetch_all_occupied_boxes() ->
    ResId = emqx_agent_skill_postgresql:resource_id(),
    {ok, _Cols, Rows} = emqx_resource:simple_sync_query(
        ResId,
        {query, <<"SELECT x, y FROM demo_storage_field_boxes ORDER BY y, x">>}
    ),
    [#{<<"x">> => X, <<"y">> => Y} || {X, Y} <- Rows].

query_ok({ok, _}) -> ok;
query_ok({ok, _, _}) -> ok.

wait_until(CheckFun, TimeoutMs) ->
    wait_until(CheckFun, TimeoutMs, 100).

wait_until(_CheckFun, TimeoutMs, _IntervalMs) when TimeoutMs =< 0 ->
    ct:fail("condition_not_met_before_timeout");
wait_until(CheckFun, TimeoutMs, IntervalMs) ->
    case CheckFun() of
        true ->
            ok;
        false ->
            timer:sleep(IntervalMs),
            wait_until(CheckFun, TimeoutMs - IntervalMs, IntervalMs)
    end.

decode_json_or_fail(Payload) ->
    case emqx_utils_json:safe_decode(Payload) of
        {ok, V} -> V;
        {error, Reason} -> ct:fail("command payload is not json: ~p ~p", [Payload, Reason])
    end.

core_command(Core) ->
    case maps:get(<<"command">>, Core, undefined) of
        undefined ->
            Summary = maps:get(<<"summary">>, Core, <<>>),
            command_from_summary(Summary);
        Cmd ->
            Cmd
    end.

command_from_summary(Summary) when is_binary(Summary) ->
    case emqx_utils_json:safe_decode(Summary) of
        {ok, #{<<"command">> := Cmd}} ->
            Cmd;
        _ ->
            Stripped = binary:replace(Summary, <<"```json\n">>, <<>>, [global]),
            Jsonish = binary:replace(Stripped, <<"\n```">>, <<>>, [global]),
            case emqx_utils_json:safe_decode(Jsonish) of
                {ok, #{<<"command">> := Cmd2}} -> Cmd2;
                _ -> ct:fail("cannot extract command from llm summary: ~p", [Summary])
            end
    end.

must_env(Name) ->
    case os:getenv(Name) of
        false -> ct:fail("required env var missing: ~s", [Name]);
        Val when is_list(Val) -> list_to_binary(Val)
    end.

env_or_default(Name, Default) ->
    case os:getenv(Name) of
        false -> Default;
        Val when is_list(Val) -> list_to_binary(Val)
    end.

ensure_fireworks_available(ApiKey, Model) ->
    Url = <<?FIREWORKS_BASE_URL/binary, "/chat/completions">>,
    Headers = [
        {<<"authorization">>, <<"Bearer ", ApiKey/binary>>},
        {<<"content-type">>, <<"application/json">>}
    ],
    Body = emqx_utils_json:encode(#{
        <<"model">> => Model,
        <<"messages">> => [#{<<"role">> => <<"user">>, <<"content">> => <<"ping">>}],
        <<"max_tokens">> => 8
    }),
    Opts = [with_body, {connect_timeout, 5_000}, {recv_timeout, 20_000}],
    case hackney:request(post, Url, Headers, Body, Opts) of
        {ok, 200, _RespHeaders, _RespBody} -> ok;
        {ok, Status, _RespHeaders, RespBody} ->
            ct:fail("fireworks probe failed status=~p body=~p", [Status, RespBody]);
        {error, Reason} ->
            ct:fail("fireworks probe failed reason=~p", [Reason])
    end.

pg_status_skill_id(Id) -> <<"pg_status_", Id/binary>>.
pg_set_status_skill_id(Id) -> <<"pg_set_status_", Id/binary>>.
pg_set_box_skill_id(Id) -> <<"pg_set_box_", Id/binary>>.
pg_field_skill_id(Id) -> <<"pg_field_", Id/binary>>.
command_skill_id(Id) -> <<"pub_cmd_", Id/binary>>.
