%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_skill_http_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(SKILL_TYPE, <<"http">>).
-define(SKILL_ID, <<"weather-api">>).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx, emqx_conf, emqx_resource, emqx_agent], #{
        work_dir => emqx_cth_suite:work_dir(Config)
    }),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_agent_plugin_config_fixture:setup(),
    {ok, {Port, _Pid}} = emqx_utils_http_test_server:start_link(random, "/", false),
    BaseUrl = iolist_to_binary(io_lib:format("http://127.0.0.1:~p/", [Port])),
    ok = emqx_agent_skill_http:init(),
    ok = register_skill(test_context([{base_url, BaseUrl} | Config])),
    [{base_url, BaseUrl} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_agent_skill_http:deinit(),
    ok = emqx_agent_plugin_config_fixture:teardown(),
    ok = emqx_utils_http_test_server:stop().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_create_returns_skill(_Config) ->
    {ok, Skill} = emqx_agent_skill_registry:lookup(?SKILL_TYPE, ?SKILL_ID),
    ?assertMatch(#{skill_id := ?SKILL_ID}, Skill),
    ?assertMatch(#{type := ?SKILL_TYPE}, Skill),
    ?assert(is_map(maps:get(input_schema, Skill))).

t_destroy_accepts_runtime_skill(_Config) ->
    {ok, Skill} = emqx_agent_skill_registry:lookup(?SKILL_TYPE, ?SKILL_ID),
    ?assertEqual(ok, emqx_agent_skill_http:destroy(Skill)).

t_append_query_empty_args(_Config) ->
    ?assertEqual(
        <<"https://api.example.com/weather">>,
        emqx_agent_skill_http:append_query(<<"https://api.example.com/weather">>, #{})
    ).

t_append_query_single_param(_Config) ->
    Url = emqx_agent_skill_http:append_query(
        <<"https://api.example.com/weather">>, #{<<"city">> => <<"London">>}
    ),
    ?assertMatch(<<"https://api.example.com/weather?", _/binary>>, Url),
    ?assertNotEqual(nomatch, binary:match(Url, <<"city=London">>)).

t_append_query_integer_param(_Config) ->
    Url = emqx_agent_skill_http:append_query(
        <<"https://api.example.com/forecast">>, #{<<"days">> => 5}
    ),
    ?assertNotEqual(nomatch, binary:match(Url, <<"days=5">>)).

t_get_invoke_publishes_reply(Config) ->
    emqx_utils_http_test_server:set_handler(fun(Req0, State) ->
        QS = maps:from_list(cowboy_req:parse_qs(Req0)),
        reply_json(
            200, #{<<"city">> => maps:get(<<"city">>, QS), <<"temperature">> => 22.5}, Req0, State
        )
    end),
    invoke_and_assert(Config, #{<<"city">> => <<"London">>, <<"units">> => <<"metric">>}, fun(Data) ->
        ?assertMatch(#{<<"city">> := <<"London">>}, Data),
        ?assertEqual(22.5, maps:get(<<"temperature">>, Data))
    end).

t_post_invoke_sends_json_body(Config) ->
    ok = emqx_agent_config:delete_skill(?SKILL_TYPE, ?SKILL_ID),
    ok = register_skill(test_context(Config, #{<<"method">> => <<"post">>})),
    emqx_utils_http_test_server:set_handler(fun(Req0, State) ->
        {ok, RawBody, Req1} = cowboy_req:read_body(Req0),
        #{<<"city">> := City} = emqx_utils_json:decode(RawBody),
        reply_json(201, #{<<"status">> => <<"created">>, <<"city">> => City}, Req1, State)
    end),
    invoke_and_assert(Config, #{<<"city">> => <<"Berlin">>, <<"units">> => null}, fun(Data) ->
        ?assertMatch(#{<<"status">> := <<"created">>, <<"city">> := <<"Berlin">>}, Data)
    end).

t_get_passes_headers_to_server(Config) ->
    Self = self(),
    emqx_utils_http_test_server:set_handler(fun(Req0, State) ->
        Self ! {request_headers, cowboy_req:headers(Req0)},
        reply_json(200, #{<<"ok">> => true}, Req0, State)
    end),
    invoke_and_assert(Config, #{<<"city">> => <<"Oslo">>, <<"units">> => null}, fun(_) -> ok end),
    receive
        {request_headers, Headers} ->
            ?assertEqual(<<"test-key-123">>, maps:get(<<"x-api-key">>, Headers))
    after 3000 ->
        ct:fail(no_request_received)
    end.

t_non_json_response_wrapped_in_raw(Config) ->
    emqx_utils_http_test_server:set_handler(fun(Req0, State) ->
        Req = cowboy_req:reply(
            200, #{<<"content-type">> => <<"text/plain">>}, <<"plain text">>, Req0
        ),
        {ok, Req, State}
    end),
    invoke_and_assert(Config, #{}, fun(Data) ->
        ?assertMatch(#{<<"raw">> := <<"plain text">>}, Data)
    end).

t_unregistered_skill_is_silently_ignored(_Config) ->
    ok = emqx_agent_config:delete_skill(?SKILL_TYPE, ?SKILL_ID),
    Msg = emqx_message:make(?SKILL_ID, 0, invoke_topic(<<"dummy-req">>), <<"ignored">>),
    ?assertMatch(#message{}, emqx_hooks:run_fold('message.publish', [], Msg)),
    ok = register_skill(test_context(_Config)).

t_non_http_topic_is_ignored(_Config) ->
    Msg = emqx_message:make(?SKILL_ID, 0, <<"other/topic">>, <<"payload">>),
    ?assertMatch(#message{}, emqx_hooks:run_fold('message.publish', [], Msg)).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

invoke_topic(ReqId) ->
    <<"$cap/http/", ?SKILL_ID/binary, "/request/", ReqId/binary>>.

reply_topic(ReqId) ->
    <<"$cap/http/", ?SKILL_ID/binary, "/response/", ReqId/binary>>.

invoke_and_assert(_Config, Args, AssertFn) ->
    ReqId = base64:encode(crypto:strong_rand_bytes(8)),
    ReplyTopic = reply_topic(ReqId),
    ok = emqx:subscribe(ReplyTopic),

    Payload = emqx_utils_json:encode(#{
        <<"trace_id">> => <<"tr-test">>,
        <<"mode">> => <<"unary">>,
        <<"args">> => Args
    }),
    _ = emqx_broker:publish(emqx_message:make(?SKILL_ID, 0, invoke_topic(ReqId), Payload)),

    Reply =
        receive
            {deliver, ReplyTopic, #message{payload = P}} -> emqx_utils_json:decode(P)
        after 3000 ->
            ct:fail(no_reply_received)
        end,

    ?assertMatch(#{<<"req_id">> := ReqId}, Reply),
    ?assertMatch(#{<<"type">> := ?SKILL_TYPE, <<"id">> := ?SKILL_ID}, maps:get(<<"skill">>, Reply)),
    AssertFn(maps:get(<<"result">>, emqx_agent_skill_helpers:cap_response(Reply))),

    ok = emqx:unsubscribe(ReplyTopic).

test_context(Config) ->
    test_context(Config, #{}).

test_context(Config, Overrides) ->
    BaseUrl = ?config(base_url, Config),
    maps:merge(
        #{
            <<"type">> => ?SKILL_TYPE,
            <<"id">> => ?SKILL_ID,
            <<"desc">> => <<"Fetch current weather for a city.">>,
            <<"method">> => <<"get">>,
            <<"url">> => BaseUrl,
            <<"headers">> => [#{<<"name">> => <<"x-api-key">>, <<"value">> => <<"test-key-123">>}],
            <<"input_schema">> => #{
                <<"type">> => <<"object">>,
                <<"properties">> => #{
                    <<"city">> => #{<<"type">> => <<"string">>},
                    <<"units">> => #{
                        <<"type">> => [<<"string">>, <<"null">>],
                        <<"enum">> => [<<"metric">>, <<"imperial">>, null]
                    }
                },
                <<"required">> => [<<"city">>, <<"units">>],
                <<"additionalProperties">> => false
            }
        },
        Overrides
    ).

register_skill(Context) ->
    Body = maybe_encode_schema(<<"input_schema">>, Context),
    emqx_agent_config:create_skill(Body).

maybe_encode_schema(Field, Body) ->
    case maps:get(Field, Body, undefined) of
        Schema when is_map(Schema) -> Body#{Field => emqx_utils_json:encode(Schema)};
        _ -> Body
    end.

reply_json(Status, Data, Req0, State) ->
    Req = cowboy_req:reply(
        Status,
        #{<<"content-type">> => <<"application/json">>},
        emqx_utils_json:encode(Data),
        Req0
    ),
    {ok, Req, State}.
