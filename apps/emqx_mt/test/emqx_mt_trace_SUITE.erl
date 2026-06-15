%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mt_trace_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("emqx/include/emqx_config.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(TCConfig) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_conf, "mqtt.client_attrs_init = [{expression = username, set_as_attr = tns}]"},
            emqx_mt,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [{apps, Apps} | TCConfig].

end_per_suite(TCConfig) ->
    Apps = ?config(apps, TCConfig),
    ok = emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_Case, TCConfig) ->
    _ = [logger:remove_handler(Id) || #{id := Id} <- emqx_trace_handler:running()],
    init(),
    TCConfig.

end_per_testcase(_Case, _TCConfig) ->
    emqx_common_test_helpers:call_janitor(),
    terminate(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

init() ->
    %% todo: trace should be moved to emqx's sup tree
    emqx_trace:start_link().

terminate() ->
    catch ok = gen_server:stop(emqx_trace, normal, 5_000).

gen_name() ->
    Name0 = uuid:uuid_to_string(uuid:get_v4(), binary_standard),
    <<"trace-", Name0/binary>>.

simple_request(Params) ->
    emqx_mgmt_api_test_util:simple_request(Params).

delete_trace(TraceName) ->
    URL = emqx_mgmt_api_test_util:api_path(["trace", TraceName]),
    simple_request(#{
        method => delete,
        url => URL
    }).

start_topic_trace(TopicFilter, Opts) ->
    Name = gen_name(),
    on_exit(fun() -> delete_trace(Name) end),
    Body = #{
        <<"name">> => Name,
        <<"type">> => <<"topic">>,
        <<"topic">> => TopicFilter,
        <<"formatter">> => <<"json">>,
        <<"payload_encode">> => <<"text">>
    },
    AuthHeader = emqx_utils_maps:get_lazy(
        auth_header,
        Opts,
        fun emqx_bridge_v2_testlib:auth_header/0
    ),
    URL = emqx_mgmt_api_test_util:api_path(["trace"]),
    simple_request(#{
        auth_header => AuthHeader,
        method => post,
        url => URL,
        body => Body
    }).

start_clientid_trace(ClientId, Opts) ->
    Name = gen_name(),
    on_exit(fun() -> delete_trace(Name) end),
    Body = #{
        <<"name">> => Name,
        <<"type">> => <<"clientid">>,
        <<"clientid">> => ClientId,
        <<"formatter">> => <<"json">>,
        <<"payload_encode">> => <<"text">>
    },
    AuthHeader = emqx_utils_maps:get_lazy(
        auth_header,
        Opts,
        fun emqx_bridge_v2_testlib:auth_header/0
    ),
    URL = emqx_mgmt_api_test_util:api_path(["trace"]),
    simple_request(#{
        auth_header => AuthHeader,
        method => post,
        url => URL,
        body => Body
    }).

start_ip_address_trace(IpAddr, Opts) ->
    Name = gen_name(),
    on_exit(fun() -> delete_trace(Name) end),
    Body = #{
        <<"name">> => Name,
        <<"type">> => <<"ip_address">>,
        <<"ip_address">> => IpAddr,
        <<"formatter">> => <<"json">>,
        <<"payload_encode">> => <<"text">>
    },
    AuthHeader = emqx_utils_maps:get_lazy(
        auth_header,
        Opts,
        fun emqx_bridge_v2_testlib:auth_header/0
    ),
    URL = emqx_mgmt_api_test_util:api_path(["trace"]),
    simple_request(#{
        auth_header => AuthHeader,
        method => post,
        url => URL,
        body => Body
    }).

get_test_trace_log(TraceName) ->
    emqx_bridge_v2_testlib:get_test_trace_log(TraceName).

get_trace_log_for_ns(TraceName, Ns) ->
    maybe
        {ok, Events} ?= get_test_trace_log(TraceName),
        lists:filter(
            fun(Event) ->
                case Event of
                    #{<<"meta">> := #{<<"namespace">> := Ns}} ->
                        true;
                    #{<<"meta">> := #{} = Meta} when
                        not is_map_key(<<"namespace">>, Meta) andalso Ns == ?global_ns
                    ->
                        true;
                    _ ->
                        false
                end
            end,
            Events
        )
    end.

ensure_managed_ns(Ns) ->
    URL = emqx_mgmt_api_test_util:api_path(["mt", "ns", Ns]),
    Res = simple_request(#{
        method => post,
        url => URL,
        body => <<"">>
    }),
    case Res of
        {204, _} ->
            ok;
        {400, #{<<"message">> := <<"already_exists">>}} ->
            ok
    end.

create_user_and_token(Opts) ->
    Token = emqx_bridge_v2_testlib:create_namespaced_user_and_token(Opts),
    {"Authorization", <<"Bearer ", Token/binary>>}.

start_client(Opts0) ->
    Default = #{proto_ver => v5},
    Opts = maps:merge(Default, Opts0),
    start_client(Opts, 10).

start_client(Opts, Retries) ->
    {ok, C} = emqtt:start_link(Opts),
    unlink(C),
    case emqtt:connect(C) of
        {ok, _} ->
            link(C),
            C;
        {error, {server_busy, _}} when Retries > 0 ->
            _ = catch emqtt:stop(C),
            ct:pal("server_busy on connect, retrying (~p left)", [Retries - 1]),
            timer:sleep(10),
            start_client(Opts, Retries - 1)
    end.

wait_filesync() ->
    %% NOTE: Twice as long as `?LOG_HANDLER_FILESYNC_INTERVAL` in `emqx_trace_handler`.
    timer:sleep(2 * 100).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc """
Checks that we filter trace events by crossing the namespace of the trace and that of the
event.
This tests traces of type topic.
""".
t_filter_topic(_TCConfig) ->
    Ns = <<"ns1">>,
    OtherNs = <<"other_ns">>,
    ok = ensure_managed_ns(Ns),
    ok = ensure_managed_ns(OtherNs),
    HeaderNs = create_user_and_token(#{}),

    Run = fun() ->
        NsClient = start_client(#{username => Ns}),
        OtherNsClient = start_client(#{username => OtherNs}),
        GlobalClient = start_client(#{}),

        {ok, _} = emqtt:publish(NsClient, <<"a/b/c">>, <<"from_ns1">>, [{qos, 1}]),
        {ok, _} = emqtt:publish(OtherNsClient, <<"a/b/c">>, <<"from_other_ns">>, [{qos, 1}]),
        {ok, _} = emqtt:publish(GlobalClient, <<"a/b/c">>, <<"from_global">>, [{qos, 1}]),

        {ok, _, _} = emqtt:subscribe(NsClient, <<"t/ns1">>, [{qos, 1}]),
        {ok, _, _} = emqtt:subscribe(OtherNsClient, <<"t/other_ns">>, [{qos, 1}]),
        {ok, _, _} = emqtt:subscribe(GlobalClient, <<"t/global">>, [{qos, 1}]),

        {ok, _, _} = emqtt:unsubscribe(NsClient, <<"t/ns1">>),
        {ok, _, _} = emqtt:unsubscribe(OtherNsClient, <<"t/other_ns">>),
        {ok, _, _} = emqtt:unsubscribe(GlobalClient, <<"t/global">>),

        lists:foreach(fun emqtt:stop/1, [NsClient, OtherNsClient, GlobalClient]),

        ok = wait_filesync(),

        ok
    end,

    {200, #{<<"name">> := TraceName}} = start_topic_trace(<<"#">>, #{auth_header => HeaderNs}),
    Run(),

    ?assertMatch(
        {ok, [
            #{
                <<"msg">> := <<"publish_to">>,
                <<"meta">> := #{<<"username">> := Ns}
            },
            #{
                <<"msg">> := <<"subscribe">>,
                <<"meta">> := #{<<"username">> := Ns}
            },
            #{
                <<"msg">> := <<"unsubscribe">>,
                <<"meta">> := #{<<"username">> := Ns}
            }
        ]},
        get_test_trace_log(TraceName)
    ),
    delete_trace(TraceName),

    %% global admin can see all namespaces
    {200, #{<<"name">> := GlobalTraceName}} = start_topic_trace(<<"#">>, #{}),
    Run(),
    ?assertMatch(
        [_ | _],
        get_trace_log_for_ns(GlobalTraceName, Ns)
    ),
    ?assertMatch(
        [_ | _],
        get_trace_log_for_ns(GlobalTraceName, OtherNs)
    ),
    ?assertMatch(
        [_ | _],
        get_trace_log_for_ns(GlobalTraceName, ?global_ns)
    ),

    ok.

-doc """
Checks that we filter trace events by crossing the namespace of the trace and that of the
event.
This tests traces of type ip_address.
""".
t_filter_ip_address(_TCConfig) ->
    Ns = <<"ns1">>,
    OtherNs = <<"other_ns">>,
    ok = ensure_managed_ns(Ns),
    ok = ensure_managed_ns(OtherNs),
    HeaderNs = create_user_and_token(#{}),

    Run = fun() ->
        NsClient = start_client(#{username => Ns}),
        OtherNsClient = start_client(#{username => OtherNs}),
        GlobalClient = start_client(#{}),

        {ok, _} = emqtt:publish(NsClient, <<"a/b/c">>, <<"from_ns1">>, [{qos, 1}]),
        {ok, _} = emqtt:publish(OtherNsClient, <<"a/b/c">>, <<"from_other_ns">>, [{qos, 1}]),
        {ok, _} = emqtt:publish(GlobalClient, <<"a/b/c">>, <<"from_global">>, [{qos, 1}]),

        {ok, _, _} = emqtt:subscribe(NsClient, <<"t/ns1">>, [{qos, 1}]),
        {ok, _, _} = emqtt:subscribe(OtherNsClient, <<"t/other_ns">>, [{qos, 1}]),
        {ok, _, _} = emqtt:subscribe(GlobalClient, <<"t/global">>, [{qos, 1}]),

        {ok, _, _} = emqtt:unsubscribe(NsClient, <<"t/ns1">>),
        {ok, _, _} = emqtt:unsubscribe(OtherNsClient, <<"t/other_ns">>),
        {ok, _, _} = emqtt:unsubscribe(GlobalClient, <<"t/global">>),

        lists:foreach(fun emqtt:stop/1, [NsClient, OtherNsClient, GlobalClient]),

        ok = wait_filesync(),

        ok
    end,

    {200, #{<<"name">> := TraceName}} = start_ip_address_trace(<<"127.0.0.1">>, #{
        auth_header => HeaderNs
    }),
    Run(),
    {ok, Events} = get_test_trace_log(TraceName),
    ?assertMatch([_ | _], Events),
    %% events outside target namespace
    ExtraEvents = lists:filter(
        fun(Event) ->
            case Event of
                #{<<"meta">> := #{<<"namespace">> := Ns}} ->
                    false;
                _ ->
                    true
            end
        end,
        Events
    ),
    ?assertEqual([], ExtraEvents),
    delete_trace(TraceName),

    %% global admin can see all namespaces
    {200, #{<<"name">> := GlobalTraceName}} = start_ip_address_trace(<<"127.0.0.1">>, #{}),
    Run(),
    ?assertMatch(
        [_ | _],
        get_trace_log_for_ns(GlobalTraceName, Ns)
    ),
    ?assertMatch(
        [_ | _],
        get_trace_log_for_ns(GlobalTraceName, OtherNs)
    ),
    ?assertMatch(
        [_ | _],
        get_trace_log_for_ns(GlobalTraceName, ?global_ns)
    ),

    ok.

-doc """
Checks that we filter trace events by crossing the namespace of the trace and that of the
event.
This tests traces of type clientid.
""".
t_filter_clientid(_TCConfig) ->
    Ns = <<"ns1">>,
    OtherNs = <<"other_ns">>,
    ok = ensure_managed_ns(Ns),
    ok = ensure_managed_ns(OtherNs),
    HeaderNs = create_user_and_token(#{}),

    Run = fun() ->
        NsClient = start_client(#{username => Ns, clientid => Ns}),
        {ok, _} = emqtt:publish(NsClient, <<"a/b/c">>, <<"from_ns1">>, [{qos, 1}]),
        {ok, _, _} = emqtt:subscribe(NsClient, <<"t/ns1">>, [{qos, 1}]),
        {ok, _, _} = emqtt:unsubscribe(NsClient, <<"t/ns1">>),
        emqtt:stop(NsClient),

        OtherNsClient = start_client(#{username => OtherNs, clientid => Ns}),
        {ok, _} = emqtt:publish(OtherNsClient, <<"a/b/c">>, <<"from_other_ns">>, [{qos, 1}]),
        {ok, _, _} = emqtt:subscribe(OtherNsClient, <<"t/other_ns">>, [{qos, 1}]),
        {ok, _, _} = emqtt:unsubscribe(OtherNsClient, <<"t/other_ns">>),
        emqtt:stop(OtherNsClient),

        GlobalClient = start_client(#{clientid => Ns}),
        {ok, _} = emqtt:publish(GlobalClient, <<"a/b/c">>, <<"from_global">>, [{qos, 1}]),
        {ok, _, _} = emqtt:subscribe(GlobalClient, <<"t/global">>, [{qos, 1}]),
        {ok, _, _} = emqtt:unsubscribe(GlobalClient, <<"t/global">>),
        emqtt:stop(GlobalClient),

        ok = wait_filesync(),

        ok
    end,

    {200, #{<<"name">> := TraceName}} = start_clientid_trace(Ns, #{auth_header => HeaderNs}),
    Run(),
    {ok, Events} = get_test_trace_log(TraceName),
    ?assertMatch([_ | _], Events),
    %% events outside target namespace
    ExtraEvents = lists:filter(
        fun(Event) ->
            case Event of
                #{<<"meta">> := #{<<"namespace">> := Ns}} ->
                    false;
                _ ->
                    true
            end
        end,
        Events
    ),
    ?assertEqual([], ExtraEvents),
    delete_trace(TraceName),

    %% global admin can see all namespaces
    {200, #{<<"name">> := GlobalTraceName}} = start_clientid_trace(Ns, #{}),
    Run(),
    ?assertMatch(
        [_ | _],
        get_trace_log_for_ns(GlobalTraceName, Ns)
    ),
    ?assertMatch(
        [_ | _],
        get_trace_log_for_ns(GlobalTraceName, OtherNs)
    ),
    ?assertMatch(
        [_ | _],
        get_trace_log_for_ns(GlobalTraceName, ?global_ns)
    ),

    ok.
