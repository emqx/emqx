%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_prometheus_SUITE).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-compile(nowarn_export_all).
-compile(export_all).

%% Process that receives the bodies captured by the mock push gateway.
-define(PUSH_BODY_RECEIVER, emqx_prometheus_suite_push_body).

%% erlfmt-ignore
-define(LEGACY_CONF_DEFAULT, <<"
prometheus {
    push_gateway_server = \"http://127.0.0.1:9091\"
    interval = \"1s\"
    headers = { Authorization = \"some-authz-tokens\"}
    job_name = \"${cluster_name}~${name}~${host}\"
    enable = true
    vm_dist_collector = disabled
    mnesia_collector = disabled
    vm_statistics_collector = disabled
    vm_system_info_collector = disabled
    vm_memory_collector = disabled
    vm_msacc_collector = disabled
}
">>).

-define(CONF_DEFAULT, #{
    <<"prometheus">> =>
        #{
            <<"enable_basic_auth">> => false,
            <<"collectors">> =>
                #{
                    <<"mnesia">> => <<"disabled">>,
                    <<"vm_dist">> => <<"disabled">>,
                    <<"vm_memory">> => <<"disabled">>,
                    <<"vm_msacc">> => <<"disabled">>,
                    <<"vm_statistics">> => <<"disabled">>,
                    <<"vm_system_info">> => <<"disabled">>
                },
            <<"push_gateway">> =>
                #{
                    <<"enable">> => true,
                    <<"headers">> => #{<<"Authorization">> => <<"some-authz-tokens">>},
                    <<"interval">> => <<"1s">>,
                    <<"job_name">> => <<"${cluster_name}~${name}~${host}">>,
                    <<"url">> => <<"http://127.0.0.1:9091">>
                }
        }
}).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------
all() ->
    [
        {group, new_config},
        {group, legacy_config}
    ].

groups() ->
    [
        {new_config, [sequence], common_tests()},
        {legacy_config, [sequence], common_tests()}
    ].

suite() ->
    [{timetrap, {seconds, 60}}].

common_tests() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_group(new_config, Config) ->
    Apps = emqx_cth_suite:start(
        [
            %% coverage olp metrics
            {emqx_conf, "overload_protection.enable = true"},
            emqx,
            {emqx_license, "license.key = default"},
            emqx_conf,
            emqx_connector,
            emqx_bridge_http,
            emqx_bridge,
            emqx_rule_engine,
            emqx_auth,
            {emqx_prometheus, #{config => config(default)}},
            emqx_mt,
            emqx_management
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config];
init_per_group(legacy_config, Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, "overload_protection.enable = true"},
            emqx,
            {emqx_license, "license.key = default"},
            emqx_conf,
            emqx_connector,
            emqx_bridge_http,
            emqx_bridge,
            emqx_rule_engine,
            emqx_auth,
            {emqx_prometheus, #{config => config(legacy)}},
            emqx_mt,
            emqx_management
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_group(_Group, Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(t_assert_push, Config) ->
    meck:new(httpc, [passthrough]),
    Config;
init_per_testcase(t_push_gateway, Config) ->
    start_mock_pushgateway(9091),
    Config;
init_per_testcase(t_topic_metrics_push_gateway, Config) ->
    start_mock_pushgateway(9091),
    true = register(?PUSH_BODY_RECEIVER, self()),
    Config;
init_per_testcase(_Testcase, Config) ->
    Config.

end_per_testcase(t_push_gateway, Config) ->
    stop_mock_pushgateway(),
    Config;
end_per_testcase(t_topic_metrics_push_gateway, Config) ->
    stop_mock_pushgateway(),
    ok = emqx_topic_metrics2:deregister_all(),
    ok = emqx_prometheus_sup:stop_child(emqx_prometheus),
    %% CT may run this in a fresh process after a timetrap.
    _ = catch unregister(?PUSH_BODY_RECEIVER),
    Config;
end_per_testcase(t_assert_push, _Config) ->
    meck:unload(httpc),
    ok;
end_per_testcase(_Testcase, _Config) ->
    ok.

config(default) ->
    ?CONF_DEFAULT;
config(legacy) ->
    ?LEGACY_CONF_DEFAULT.

conf_default() ->
    ?CONF_DEFAULT.

legacy_conf_default() ->
    ?LEGACY_CONF_DEFAULT.

mock_license() ->
    meck:new(emqx_license_checker, [non_strict, passthrough, no_link]),
    meck:expect(emqx_license_checker, expiry_epoch, fun() -> 1859673600 end).

unmock_license() ->
    meck:unload(emqx_license_checker).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_start_stop(_) ->
    App = emqx_prometheus,
    Conf = emqx_prometheus_config:conf(),
    ?assertMatch(ok, emqx_prometheus_sup:start_child(App, Conf)),
    %% start twice return ok.
    ?assertMatch(ok, emqx_prometheus_sup:start_child(App, Conf)),
    ok = gen_server:call(emqx_prometheus, dump, 1000),
    ok = gen_server:cast(emqx_prometheus, dump),
    dump = erlang:send(emqx_prometheus, dump),
    ?assertMatch(ok, emqx_prometheus_sup:stop_child(App)),
    %% stop twice return ok.
    ?assertMatch(ok, emqx_prometheus_sup:stop_child(App)),
    %% wait the interval timer trigger
    timer:sleep(2000).

t_collector_no_crash_test(_) ->
    prometheus_text_format:format(),
    ok.

t_authz_matched_metrics(_) ->
    ?assert(
        lists:member(
            {emqx_authorization_matched_allow, counter, 'authorization.matched.allow'},
            emqx_prometheus:acl_metric_meta()
        )
    ),
    ?assert(
        lists:member(
            {emqx_authorization_matched_deny, counter, 'authorization.matched.deny'},
            emqx_prometheus:acl_metric_meta()
        )
    ).

t_assert_push(_) ->
    Self = self(),
    AssertPush = fun(Method, Req = {Url, Headers, ContentType, Data}, HttpOpts, Opts) ->
        ?assertEqual(put, Method),
        ?assertMatch("http://127.0.0.1:9091/metrics/job/emqxcl~test~127.0.0.1", Url),
        ?assertEqual([{"Authorization", "some-authz-tokens"}], Headers),
        ?assertEqual("text/plain", ContentType),
        ?assertEqual(true, assert_push_gateway_data(Data)),
        Self ! pass,
        meck:passthrough([Method, Req, HttpOpts, Opts])
    end,
    meck:expect(httpc, request, AssertPush),
    Conf = emqx_prometheus_config:conf(),
    ?assertMatch(ok, emqx_prometheus_sup:start_child(emqx_prometheus, Conf)),
    receive
        pass -> ok
    after 2000 ->
        ct:fail(assert_push_request_failed)
    end.

t_push_gateway(_) ->
    Conf = emqx_prometheus_config:conf(),
    ?assertMatch(ok, emqx_prometheus_sup:stop_child(emqx_prometheus)),
    ?assertMatch(ok, emqx_prometheus_sup:start_child(emqx_prometheus, Conf)),
    ?assertMatch(#{ok := 0, failed := 0}, emqx_prometheus:info()),
    timer:sleep(1100),
    ?assertMatch(#{ok := 1, failed := 0}, emqx_prometheus:info()),
    ok = emqx_prometheus_sup:update_child(emqx_prometheus, Conf),
    ?assertMatch(#{ok := 0, failed := 0}, emqx_prometheus:info()),

    ok.

t_cert_expiry_epoch(_) ->
    Path = some_pem_path(),
    ?assertEqual(
        2666082573,
        emqx_prometheus:cert_expiry_at_from_path(Path)
    ).

-doc """
Checks that the push gateway, which renders every registry without a request scope, still
pushes the topic-metric collections of every namespace: the context-free entry point must
behave like the unscoped API view.
""".
t_topic_metrics_push_gateway(_) ->
    %% Another case may have left the pusher stopped; run it only for this
    %% fixture so the first collected body is guaranteed to be fresh.
    Conf = emqx_prometheus_config:conf(),
    ?assertMatch(ok, emqx_prometheus_sup:stop_child(emqx_prometheus)),

    ok = emqx_topic_metrics2:register(<<"metric-a">>, <<"tenant/a/#">>, <<"ns1">>),
    ok = emqx_topic_metrics2:register(<<"metric-b">>, <<"tenant/b/#">>, <<"ns2">>),
    ok = emqx_topic_metrics2:register(<<"metric-g">>, <<"global/#">>, ?global_ns),
    publish_as(<<"ns1">>, <<"tenant/a/1">>),
    publish_as(<<"ns2">>, <<"tenant/b/1">>),
    publish_as(<<"ns1">>, <<"global/1">>),

    Expected = topic_metrics_expected([
        {<<"metric-a">>, <<"tenant/a/#">>, <<"ns1">>, 1, 10, 1},
        {<<"metric-b">>, <<"tenant/b/#">>, <<"ns2">>, 1, 10, 1},
        {<<"metric-g">>, <<"global/#">>, undefined, 1, 8, 1}
    ]),
    ?assertMatch(ok, emqx_prometheus_sup:start_child(emqx_prometheus, Conf)),
    Body = wait_push_body(fun(B) -> parse_topic_metrics(B) =:= Expected end, 50),
    ?assertEqual(Expected, parse_topic_metrics(Body)),
    ok.

%%--------------------------------------------------------------------
%% Helper functions

start_mock_pushgateway(Port) ->
    ensure_loaded(cowboy),
    ensure_loaded(ranch),
    {ok, _} = application:ensure_all_started(cowboy),
    Dispatch = cowboy_router:compile([{'_', [{'_', ?MODULE, []}]}]),
    {ok, _} = cowboy:start_clear(
        mock_pushgateway_listener,
        [{port, Port}],
        #{env => #{dispatch => Dispatch}}
    ).

ensure_loaded(App) ->
    case application:load(App) of
        ok -> ok;
        {error, {already_loaded, _}} -> ok
    end.

stop_mock_pushgateway() ->
    cowboy:stop_listener(mock_pushgateway_listener),
    ok = application:stop(cowboy),
    ok = application:stop(ranch).

init(Req0, Opts) ->
    Method = cowboy_req:method(Req0),
    Headers = cowboy_req:headers(Req0),
    ?assertEqual(<<"PUT">>, Method),
    ?assertMatch(
        #{
            <<"authorization">> := <<"some-authz-tokens">>,
            <<"content-length">> := _,
            <<"content-type">> := <<"text/plain">>,
            <<"host">> := <<"127.0.0.1:9091">>
        },
        Headers
    ),
    {ok, Body, Req1} = cowboy_req:read_body(Req0),
    ok = forward_push_body(Body),
    RespHeader = #{<<"content-type">> => <<"text/plain; charset=utf-8">>},
    Req = cowboy_req:reply(200, RespHeader, <<"OK">>, Req1),
    {ok, Req, Opts}.

forward_push_body(Body) ->
    case whereis(?PUSH_BODY_RECEIVER) of
        Pid when is_pid(Pid) ->
            Pid ! {push_body, Body},
            ok;
        undefined ->
            ok
    end.

some_pem_path() ->
    Dir = code:lib_dir(emqx_prometheus),
    _Path = filename:join([Dir, "test", "data", "cert.crt"]).

assert_push_gateway_data(Data) ->
    assert_push_gateway_data(
        [
            <<"emqx_authn_enable">>,
            <<"emqx_authz_enable">>,
            <<"emqx_rules_count">>,
            <<"emqx_actions_count">>,
            <<"emqx_connectors_count">>
        ],
        Data
    ).

assert_push_gateway_data([], _Data) ->
    true;
assert_push_gateway_data([Keyword | Keywords], Data) ->
    case re:run(Data, Keyword, [{capture, none}, global]) of
        match ->
            assert_push_gateway_data(Keywords, Data);
        nomatch ->
            {false, Keyword}
    end.

%% Wait for a pushed body that satisfies `Pred', dropping pushes that
%% were produced before the fixture was in place.  The last body seen is
%% kept so a timeout can tell "no push arrived" from "a push arrived with
%% unexpected content".
wait_push_body(Pred, Retries) ->
    wait_push_body(Pred, Retries, none).

wait_push_body(Pred, Retries, LastSeen) ->
    receive
        {push_body, Body} ->
            case Pred(Body) of
                true -> Body;
                false -> wait_push_body(Pred, Retries, Body)
            end
    after 100 ->
        case Retries of
            0 -> ct:fail({push_gateway_body_not_observed, describe_body(LastSeen)});
            _ -> wait_push_body(Pred, Retries - 1, LastSeen)
        end
    end.

describe_body(none) ->
    no_body_received;
describe_body(Body) ->
    %% Only the `messages.in' family is needed to tell which collections
    %% arrived and with which counters.
    maps:get(<<"emqx_topic_metric_messages_in_count">>, parse_topic_metrics(Body), #{}).

%% Publish a message over the public broker path, attributed to a
%% namespace through `client_attrs.tns'.
publish_as(Namespace, Topic) ->
    Msg = emqx_message:set_headers(
        #{client_attrs => #{?CLIENT_ATTR_NAME_TNS => Namespace}},
        emqx_message:make(<<"pushgateway-test">>, Topic, <<>>)
    ),
    _ = emqx_broker:publish(Msg),
    ok.

%% The topic-metric samples of a push body.  The body concatenates every
%% registry, so select only this family before parsing.
parse_topic_metrics(Body) ->
    lists:foldl(
        fun(Line, Acc) ->
            {Name, Labels, Value} = parse_prometheus_line(Line),
            maps:update_with(Name, fun(Old) -> Old#{Labels => Value} end, #{Labels => Value}, Acc)
        end,
        #{},
        [
            Line
         || Line <- binary:split(iolist_to_binary(Body), <<"\n">>, [global, trim_all]),
            is_topic_metric_line(Line)
        ]
    ).

is_topic_metric_line(<<"emqx_topic_metric_", _/binary>>) -> true;
is_topic_metric_line(_) -> false.

parse_prometheus_line(Line) ->
    RE = <<"(?<name>[a-z0-9A-Z_]+)(\\{(?<labels>[^)]*)\\})? *(?<value>[0-9]+(\\.[0-9]+)?)">>,
    {match, [Name, Labels0, Value0]} = re:run(
        Line, RE, [{capture, [<<"name">>, <<"labels">>, <<"value">>], binary}]
    ),
    Labels = parse_prometheus_labels(Labels0),
    Value =
        try
            binary_to_float(Value0)
        catch
            error:badarg ->
                binary_to_integer(Value0)
        end,
    {Name, Labels, Value}.

parse_prometheus_labels(<<"">>) ->
    #{};
parse_prometheus_labels(Labels) ->
    lists:foldl(
        fun(Label, Acc) ->
            [K, V0] = binary:split(Label, <<"=">>),
            V = binary:replace(V0, <<"\"">>, <<"">>, [global]),
            Acc#{K => V}
        end,
        #{},
        binary:split(Labels, <<",">>, [global])
    ).

%% Expected topic-metric exposition for `{BinName, TopicFilter, OwnerNs,
%% MessagesIn, BytesIn, MessagesDropped}' collections; `OwnerNs =
%% undefined' is a global-owned collection, which carries no namespace
%% label.  The fixture has no subscriber, so every publish is also
%% counted as dropped.
topic_metrics_expected(Collections) ->
    lists:foldl(
        fun({BinName, TopicFilter, OwnerNs, MessagesIn, BytesIn, MessagesDropped}, Acc0) ->
            Labels0 = #{<<"name">> => BinName, <<"topic_filter">> => TopicFilter},
            Labels =
                case OwnerNs of
                    undefined -> Labels0;
                    _ -> Labels0#{<<"namespace">> => OwnerNs}
                end,
            lists:foldl(
                fun({MetricName, Value}, Acc) ->
                    maps:update_with(
                        MetricName,
                        fun(Series) -> Series#{Labels => Value} end,
                        #{Labels => Value},
                        Acc
                    )
                end,
                Acc0,
                [
                    {<<"emqx_topic_metric_messages_in_count">>, MessagesIn},
                    {<<"emqx_topic_metric_messages_out_count">>, 0},
                    {<<"emqx_topic_metric_messages_dropped_count">>, MessagesDropped},
                    {<<"emqx_topic_metric_bytes_in">>, BytesIn},
                    {<<"emqx_topic_metric_bytes_out">>, 0}
                ]
            )
        end,
        #{},
        Collections
    ).
