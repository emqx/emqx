%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    Apps = emqx_cth_suite:start([emqx_agent], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    {ok, {Port, _Pid}} = emqx_utils_http_test_server:start_link(random, "/", false),
    BaseUrl = iolist_to_binary(io_lib:format("http://127.0.0.1:~p/", [Port])),
    ok = emqx_agent_skill_http:init(),
    ok = emqx_agent_skill_http:create(test_context([{base_url, BaseUrl} | Config])),
    [{base_url, BaseUrl} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_agent_skill_http:destroy(?SKILL_ID),
    ok = emqx_agent_skill_http:deinit(),
    ok = emqx_utils_http_test_server:stop().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_create_registers_skill(_Config) ->
    {ok, Skill} = emqx_agent_skill_registry:lookup(?SKILL_TYPE, ?SKILL_ID),
    ?assertEqual(?SKILL_ID, maps:get(skill_id, Skill)),
    ?assertEqual(?SKILL_TYPE, maps:get(type, Skill)),
    ?assert(is_map(maps:get(input_schema, Skill))),
    ?assert(is_map(maps:get(output_schema, Skill))).

t_destroy_unregisters_skill(_Config) ->
    ok = emqx_agent_skill_http:destroy(?SKILL_ID),
    ?assertEqual({error, not_found}, emqx_agent_skill_registry:lookup(?SKILL_TYPE, ?SKILL_ID)),
    %% Re-create so end_per_testcase doesn't crash
    ok = emqx_agent_skill_http:create(test_context(_Config)).

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
        ?assertEqual(<<"London">>, maps:get(<<"city">>, Data)),
        ?assertEqual(22.5, maps:get(<<"temperature">>, Data))
    end).

t_post_invoke_sends_json_body(Config) ->
    ok = emqx_agent_skill_http:destroy(?SKILL_ID),
    ok = emqx_agent_skill_http:create(test_context(Config, #{method => post})),
    emqx_utils_http_test_server:set_handler(fun(Req0, State) ->
        {ok, RawBody, Req1} = cowboy_req:read_body(Req0),
        #{<<"city">> := City} = emqx_utils_json:decode(RawBody),
        reply_json(201, #{<<"status">> => <<"created">>, <<"city">> => City}, Req1, State)
    end),
    invoke_and_assert(Config, #{<<"city">> => <<"Berlin">>}, fun(Data) ->
        ?assertMatch(#{<<"status">> := <<"created">>, <<"city">> := <<"Berlin">>}, Data)
    end).

t_get_passes_headers_to_server(Config) ->
    Self = self(),
    emqx_utils_http_test_server:set_handler(fun(Req0, State) ->
        Self ! {request_headers, cowboy_req:headers(Req0)},
        reply_json(200, #{<<"ok">> => true}, Req0, State)
    end),
    invoke_and_assert(Config, #{<<"city">> => <<"Oslo">>}, fun(_) -> ok end),
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
    ok = emqx_agent_skill_http:destroy(?SKILL_ID),
    Msg = emqx_message:make(?SKILL_ID, 0, invoke_topic(), <<"ignored">>),
    ?assertMatch(#message{}, emqx_hooks:run_fold('message.publish', [], Msg)),
    ok = emqx_agent_skill_http:create(test_context(_Config)).

t_non_http_topic_is_ignored(_Config) ->
    Msg = emqx_message:make(?SKILL_ID, 0, <<"other/topic">>, <<"payload">>),
    ?assertMatch(#message{}, emqx_hooks:run_fold('message.publish', [], Msg)).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

invoke_topic() ->
    <<"cap/invoke/http/", ?SKILL_ID/binary, "/request">>.

reply_topic(ReqId) ->
    <<"cap/invoke/http/", ?SKILL_ID/binary, "/response/", ReqId/binary>>.

invoke_and_assert(_Config, Args, AssertFn) ->
    ReqId = base64:encode(crypto:strong_rand_bytes(8)),
    ReplyTopic = reply_topic(ReqId),
    ok = emqx:subscribe(ReplyTopic),

    Payload = emqx_utils_json:encode(#{
        <<"req_id">> => ReqId,
        <<"trace_id">> => <<"tr-test">>,
        <<"mode">> => <<"unary">>,
        <<"args">> => Args
    }),
    _ = emqx_broker:publish(emqx_message:make(?SKILL_ID, 0, invoke_topic(), Payload)),

    Reply =
        receive
            {deliver, ReplyTopic, #message{payload = P}} -> emqx_utils_json:decode(P)
        after 3000 ->
            ct:fail(no_reply_received)
        end,

    ?assertEqual(ReqId, maps:get(<<"req_id">>, Reply)),
    ?assertMatch(#{<<"type">> := ?SKILL_TYPE, <<"id">> := ?SKILL_ID}, maps:get(<<"skill">>, Reply)),
    AssertFn(maps:get(<<"data">>, Reply)),

    ok = emqx:unsubscribe(ReplyTopic).

test_context(Config) ->
    test_context(Config, #{}).

test_context(Config, Overrides) ->
    BaseUrl = ?config(base_url, Config),
    maps:merge(
        #{
            skill_id => ?SKILL_ID,
            desc => <<"Fetch current weather for a city.">>,
            method => get,
            url => BaseUrl,
            headers => #{<<"x-api-key">> => <<"test-key-123">>},
            input_schema => #{
                <<"type">> => <<"object">>,
                <<"properties">> => #{
                    <<"city">> => #{<<"type">> => <<"string">>},
                    <<"units">> => #{
                        <<"type">> => <<"string">>,
                        <<"enum">> => [<<"metric">>, <<"imperial">>]
                    }
                },
                <<"required">> => [<<"city">>]
            },
            output_schema => #{
                <<"type">> => <<"object">>,
                <<"properties">> => #{
                    <<"temperature">> => #{<<"type">> => <<"number">>},
                    <<"unit">> => #{<<"type">> => <<"string">>}
                },
                <<"required">> => [<<"temperature">>]
            }
        },
        Overrides
    ).

reply_json(Status, Data, Req0, State) ->
    Req = cowboy_req:reply(
        Status,
        #{<<"content-type">> => <<"application/json">>},
        emqx_utils_json:encode(Data),
        Req0
    ),
    {ok, Req, State}.
