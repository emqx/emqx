%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_image_fetch_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(TOOL_TYPE, <<"image__fetch">>).
-define(TOOL_ID, <<"box-camera">>).

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
    {ok, {Port, _Pid}} = emqx_utils_http_test_server:start_link(random, "/[...]", false),
    BaseUrl = iolist_to_binary(io_lib:format("http://127.0.0.1:~p/images", [Port])),
    ok = emqx_agent_tool_image_fetch:init(),
    ok = register_tool(test_context([{base_url, BaseUrl} | Config])),
    [{base_url, BaseUrl} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_agent_tool_image_fetch:deinit(),
    ok = emqx_agent_plugin_config_fixture:teardown(),
    ok = emqx_utils_http_test_server:stop().

t_create_returns_tool(_Config) ->
    {ok, Tool} = emqx_agent_tool_registry:lookup(?TOOL_TYPE, ?TOOL_ID),
    ?assertMatch(#{tool_id := ?TOOL_ID, type := ?TOOL_TYPE}, Tool),
    #{description := Desc, input_schema := InputSchema, context := #{<<"url">> := Url}} = Tool,
    ?assertNotEqual(nomatch, binary:match(Desc, Url)),
    ?assertMatch(
        #{
            <<"properties">> := #{<<"url">> := #{<<"description">> := _}},
            <<"required">> := [<<"url">>]
        },
        InputSchema
    ).

t_invoke_returns_url_attachment_without_http_request(Config) ->
    Self = self(),
    emqx_utils_http_test_server:set_handler(fun(Req0, State) ->
        Self ! unexpected_request,
        Req = cowboy_req:reply(500, #{}, <<"unexpected">>, Req0),
        {ok, Req, State}
    end),
    BaseUrl = ?config(base_url, Config),
    ImageUrl = <<BaseUrl/binary, "/box-1.png?sig=abc">>,
    invoke_and_assert_response(Config, #{<<"url">> => ImageUrl}, fun(Response) ->
        ?assertMatch(
            #{
                <<"status">> := <<"ok">>,
                <<"result">> := #{<<"url">> := ImageUrl, <<"image">> := <<"Image .url">>},
                <<"attachments">> := [
                    #{<<"id">> := <<".url">>, <<"type">> := <<"image">>, <<"url">> := ImageUrl}
                ]
            },
            Response
        )
    end),
    receive
        unexpected_request -> ct:fail(unexpected_request)
    after 100 ->
        ok
    end.

t_url_outside_configured_prefix_errors(Config) ->
    BaseUrl = ?config(base_url, Config),
    [Origin, _] = binary:split(BaseUrl, <<"/images">>),
    OtherUrl = <<Origin/binary, "/other/box-1.png">>,
    invoke_and_assert_response(Config, #{<<"url">> => OtherUrl}, fun(Response) ->
        ?assertMatch(
            #{<<"status">> := <<"error">>, <<"reason">> := Reason} when is_binary(Reason),
            Response
        ),
        ?assertNotEqual(
            nomatch,
            binary:match(maps:get(<<"reason">>, Response), <<"url_outside_configured_prefix">>)
        )
    end).

t_url_outside_configured_origin_errors(_Config) ->
    invoke_and_assert_response(
        _Config, #{<<"url">> => <<"http://example.com/images/box.png">>}, fun(
            Response
        ) ->
            ?assertMatch(
                #{<<"status">> := <<"error">>, <<"reason">> := Reason} when is_binary(Reason),
                Response
            ),
            ?assertNotEqual(
                nomatch,
                binary:match(maps:get(<<"reason">>, Response), <<"url_outside_configured_origin">>)
            )
        end
    ).

t_session_renders_url_attachment_as_image_url(_Config) ->
    ImageUrl = <<"https://example.com/images/box.png">>,
    ?assertEqual(
        [
            #{
                <<"role">> => <<"user">>,
                <<"content">> => [
                    #{<<"type">> => <<"text">>, <<"text">> => <<"Tool response image(s): .url">>},
                    #{<<"type">> => <<"image_url">>, <<"image_url">> => #{<<"url">> => ImageUrl}}
                ]
            }
        ],
        emqx_agent_session:test_attachment_msgs([
            #{<<"id">> => <<".url">>, <<"type">> => <<"image">>, <<"url">> => ImageUrl}
        ])
    ).

t_session_orders_all_tool_messages_before_attachment_messages(_Config) ->
    Tool1 = #{
        <<"role">> => <<"tool">>, <<"tool_call_id">> => <<"call-1">>, <<"content">> => <<"{}">>
    },
    Tool2 = #{
        <<"role">> => <<"tool">>, <<"tool_call_id">> => <<"call-2">>, <<"content">> => <<"{}">>
    },
    Extra1 = #{<<"role">> => <<"user">>, <<"content">> => [#{<<"type">> => <<"image_url">>}]},
    Extra2 = #{<<"role">> => <<"user">>, <<"content">> => [#{<<"type">> => <<"image_url">>}]},
    ?assertEqual(
        [Tool1, Tool2, Extra1, Extra2],
        emqx_agent_session:test_order_tool_result_msgs([Tool1, Extra1, Tool2, Extra2])
    ).

invoke_topic(ReqId) ->
    <<"$cap/image__fetch/", ?TOOL_ID/binary, "/request/", ReqId/binary>>.

reply_topic(ReqId) ->
    <<"$cap/image__fetch/", ?TOOL_ID/binary, "/response/", ReqId/binary>>.

invoke_and_assert_response(_Config, Args, AssertFn) ->
    ReqId = base64:encode(crypto:strong_rand_bytes(8)),
    ReplyTopic = reply_topic(ReqId),
    ok = emqx:subscribe(ReplyTopic),

    Payload = emqx_utils_json:encode(#{
        <<"trace_id">> => <<"tr-test">>,
        <<"mode">> => <<"unary">>,
        <<"args">> => Args
    }),
    _ = emqx_broker:publish(emqx_message:make(?TOOL_ID, 0, invoke_topic(ReqId), Payload)),

    Reply =
        receive
            {deliver, ReplyTopic, #message{payload = P}} -> emqx_utils_json:decode(P)
        after 3000 ->
            ct:fail(no_reply_received)
        end,

    ?assertMatch(#{<<"req_id">> := ReqId}, Reply),
    ?assertMatch(#{<<"type">> := ?TOOL_TYPE, <<"id">> := ?TOOL_ID}, maps:get(<<"tool">>, Reply)),
    AssertFn(emqx_agent_tool_helpers:cap_response(Reply)),

    ok = emqx:unsubscribe(ReplyTopic).

test_context(Config) ->
    BaseUrl = ?config(base_url, Config),
    #{
        <<"type">> => ?TOOL_TYPE,
        <<"id">> => ?TOOL_ID,
        <<"desc">> => <<"Validate box camera image URL.">>,
        <<"url">> => BaseUrl
    }.

register_tool(Context) ->
    emqx_agent_config:create_tool(Context).
