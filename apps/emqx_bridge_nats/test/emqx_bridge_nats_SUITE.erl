-module(emqx_bridge_nats_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("../src/emqx_bridge_nats.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

all() -> [{group, local}].

groups() -> [{local, [], [t_core_publish, t_jetstream_publish, t_reconnect]}].

init_per_suite(Config) ->
    case os:find_executable("nats-server") of
        false ->
            {skip, "nats-server executable is unavailable"};
        Executable ->
            Port = free_port(),
            Pid = start_nats(Executable, Port, true),
            wait_for_port(Port),
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    emqx_conf,
                    emqx_bridge_nats,
                    emqx_bridge,
                    emqx_rule_engine,
                    emqx_management,
                    emqx_mgmt_api_test_util:emqx_dashboard()
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config)}
            ),
            [
                {apps, Apps},
                {nats_pid, Pid},
                {nats_executable, Executable},
                {nats_port, Port}
                | Config
            ]
    end.
end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)),
    stop_nats(?config(nats_pid, Config)),
    ok.
init_per_testcase(TestCase, Config) ->
    Name = atom_to_binary(TestCase),
    Port = integer_to_binary(?config(nats_port, Config)),
    ConnectorConfig = emqx_bridge_v2_testlib:parse_and_check_connector(
        ?CONNECTOR_TYPE_BIN,
        Name,
        #{
            <<"enable">> => true,
            <<"servers">> => <<"127.0.0.1:", Port/binary>>,
            <<"pool_size">> => 1,
            <<"connect_timeout">> => <<"2s">>,
            <<"authentication">> => <<"none">>,
            <<"resource_opts">> => #{<<"health_check_interval">> => <<"1s">>}
        }
    ),
    ActionConfig = emqx_bridge_v2_testlib:parse_and_check(
        action,
        ?ACTION_TYPE,
        Name,
        #{
            <<"enable">> => true,
            <<"connector">> => Name,
            <<"parameters">> => #{
                <<"subject">> => <<"emqx.events">>,
                <<"payload_template">> => <<"${.payload}">>,
                <<"headers">> => []
            },
            <<"resource_opts">> => #{
                <<"query_mode">> => <<"sync">>,
                <<"batch_size">> => 10,
                <<"batch_time">> => <<"100ms">>,
                <<"request_ttl">> => <<"60s">>
            }
        }
    ),
    on_exit(fun emqx_bridge_v2_testlib:delete_all_bridges_and_connectors/0),
    [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, Name},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, Name},
        {action_config, ActionConfig}
        | Config
    ].

end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    ok.

t_core_publish(Config) ->
    {ok, Client} = nats_client(Config),
    {ok, _} = enats_client:subscribe(Client, <<"emqx.sensor/1/data">>, #{}),
    {201, _} = create_connector(Config),
    Action = #{
        <<"parameters">> => #{
            <<"subject">> => <<"emqx.${.topic}">>,
            <<"payload_template">> => <<"${.payload}">>,
            <<"headers">> => [
                #{<<"key">> => <<"x-topic">>, <<"value">> => <<"${.topic}">>}
            ]
        }
    },
    {201, _} = create_action(Config, Action),
    {ok, _} = create_rule(Config, <<"sensor/+/data">>),
    emqx:publish(emqx_message:make(<<"sensor/1/data">>, <<"hello-core">>)),
    emqx:publish(emqx_message:make(<<"sensor/1/data">>, <<"hello-core">>)),
    emqx:publish(emqx_message:make(<<"sensor/1/data">>, <<"hello-core">>)),
    lists:foreach(
        fun(_) ->
            ?assertMatch(
                {enats_client, Client,
                    {message, #{
                        subject := <<"emqx.sensor/1/data">>,
                        payload := <<"hello-core">>,
                        headers := [{<<"x-topic">>, <<"sensor/1/data">>}]
                    }}},
                receive_message()
            )
        end,
        lists:seq(1, 3)
    ),
    ok = enats_client:stop(Client).

t_jetstream_publish(Config) ->
    {ok, Client} = nats_client(Config),
    ok = create_stream(Client),
    {201, _} = create_connector(Config),
    Action = #{
        <<"parameters">> => #{
            <<"subject">> => <<"emqx.events">>,
            <<"payload_template">> => <<"${.payload}">>,
            <<"delivery_mode">> => <<"jetstream">>,
            <<"msg_id_template">> => <<"fixed-test-id">>,
            <<"headers">> => []
        }
    },
    {201, _} = create_action(Config, Action),
    {ok, _} = create_rule(Config, <<"sensor/+/data">>),
    emqx:publish(emqx_message:make(<<"sensor/1/data">>, <<"hello-js">>)),
    emqx:publish(emqx_message:make(<<"sensor/1/data">>, <<"hello-js">>)),
    timer:sleep(200),
    ?assertEqual({ok, 1}, stream_last_sequence(Client)),
    ok = enats_client:stop(Client).

t_reconnect(Config) ->
    {ok, Client} = nats_client(Config),
    {ok, _} = enats_client:subscribe(Client, <<"emqx.events">>, #{}),
    {201, _} = create_connector(Config),
    {201, _} = create_action(Config),
    {ok, _} = create_rule(Config, <<"sensor/+/data">>),
    stop_nats(?config(nats_pid, Config)),
    receive
        {enats_client, Client, disconnected, _Reason} -> ok
    after 2000 -> ct:fail(nats_disconnect_not_observed)
    end,
    Parent = self(),
    Restart = spawn(fun() ->
        timer:sleep(200),
        Nats = start_nats(?config(nats_executable, Config), ?config(nats_port, Config), false),
        wait_for_port(?config(nats_port, Config)),
        Parent ! {nats_restarted, self()},
        receive
            stop -> stop_nats(Nats)
        end
    end),
    emqx:publish(emqx_message:make(<<"sensor/1/data">>, <<"hello-during-outage">>)),
    receive
        {nats_restarted, Restart} -> ok
    after 10000 -> ct:fail(nats_restart_not_observed)
    end,
    ?assertMatch(
        {enats_client, Client, {message, #{payload := <<"hello-during-outage">>}}},
        receive_message(5000)
    ),
    Restart ! stop,
    ok = enats_client:stop(Client).

create_connector(Config) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(Config, #{})
    ).

create_action(Config) -> create_action(Config, #{}).
create_action(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_kind_api(Config, Overrides)
    ).

create_rule(Config, Topic) ->
    emqx_bridge_v2_testlib:create_rule_and_action_http(?ACTION_TYPE_BIN, Topic, Config, #{}).

nats_client(Config) ->
    connect_client(
        enats_client:start_link(#{
            host => "127.0.0.1",
            port => ?config(nats_port, Config),
            owner => self(),
            reconnect => true,
            reconnect_delay => 100
        })
    ).

connect_client({ok, Client}) ->
    ok = enats_client:connect(Client),
    {ok, Client}.

create_stream(Client) ->
    Body = jsx:encode(#{<<"name">> => <<"EMQX">>, <<"subjects">> => [<<"emqx.events">>]}),
    {ok, _} = enats_client:request(Client, <<"$JS.API.STREAM.CREATE.EMQX">>, Body, #{
        timeout => 2000
    }),
    ok.

stream_last_sequence(Client) ->
    {ok, #{payload := Payload}} = enats_client:request(
        Client, <<"$JS.API.STREAM.INFO.EMQX">>, <<>>, #{timeout => 2000}
    ),
    #{<<"state">> := #{<<"messages">> := Count}} = jsx:decode(Payload, [return_maps]),
    {ok, Count}.

receive_message() -> receive_message(2000).
receive_message(Timeout) ->
    receive
        {enats_client, Client, {message, _} = Message} -> {enats_client, Client, Message};
        _Other -> receive_message(Timeout)
    after Timeout -> ct:fail(nats_message_timeout)
    end.

free_port() ->
    {ok, Socket} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, {_Address, Port}} = inet:sockname(Socket),
    gen_tcp:close(Socket),
    Port.

start_nats(Executable, Port, JetStream) ->
    PidFile = filename:join(
        "/tmp", "emqx-nats-" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".pid"
    ),
    Args =
        ["-a", "127.0.0.1", "-p", integer_to_list(Port), "-P", PidFile] ++
            case JetStream of
                true ->
                    StoreDir = filename:join(
                        "/tmp", "emqx-nats-" ++ integer_to_list(erlang:unique_integer([positive]))
                    ),
                    filelib:ensure_dir(filename:join(StoreDir, "placeholder")),
                    ["-js", "-sd", StoreDir];
                false ->
                    []
            end,
    {open_port({spawn_executable, Executable}, [{args, Args}, exit_status]), Port, PidFile}.

stop_nats({PortHandle, _Port, PidFile}) when is_port(PortHandle) ->
    case file:read_file(PidFile) of
        {ok, PidBin} -> os:cmd("kill -KILL " ++ string:trim(binary_to_list(PidBin)));
        {error, _} -> ok
    end,
    %% Do not synchronously close the open_port here.  On some systems
    %% port_close/1 waits for the nats-server port program and can delay the
    %% restart until the action request TTL expires.
    _ = file:delete(PidFile),
    ok;
stop_nats(_) ->
    ok.

wait_for_port(Port) ->
    case gen_tcp:connect("127.0.0.1", Port, [], 100) of
        {ok, Socket} ->
            gen_tcp:close(Socket),
            ok;
        {error, _} ->
            timer:sleep(50),
            wait_for_port(Port)
    end.
