%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(
    emqx_mgmt_api_test_util,
    [
        request/2,
        request/3,
        uri/1
    ]
).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx,
            {emqx_mq, emqx_mq_test_utils:config()},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_CaseName, Config) ->
    ok = emqx_mq_test_utils:cleanup_mqs(),
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(_CaseName, _Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_mq_test_utils:cleanup_mqs().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_crud(_Config) ->
    ?assertMatch(
        {ok, 200, []},
        api_get([message_queues])
    ),
    ?assertMatch(
        {ok, 404, _},
        api_get([message_queues, urlencode(<<"t/1">>)])
    ),
    ?assertMatch(
        {ok, 200, _},
        api_post([message_queues], #{<<"topic_filter">> => <<"t/1">>, <<"ping_interval">> => 9999})
    ),
    ?assertMatch(
        {ok, 200, [#{<<"topic_filter">> := <<"t/1">>, <<"ping_interval">> := 9999}]},
        api_get([message_queues])
    ),
    ?assertMatch(
        {ok, 404, _},
        api_put([message_queues, urlencode(<<"t/2">>)], #{<<"ping_interval">> => 10000})
    ),
    ?assertMatch(
        {ok, 200, #{<<"topic_filter">> := <<"t/1">>, <<"ping_interval">> := 10000}},
        api_put([message_queues, urlencode(<<"t/1">>)], #{<<"ping_interval">> => 10000})
    ),
    ?assertMatch(
        {ok, 200, [#{<<"topic_filter">> := <<"t/1">>, <<"ping_interval">> := 10000}]},
        api_get([message_queues])
    ),
    ?assertMatch(
        {ok, 200, #{<<"topic_filter">> := <<"t/1">>, <<"ping_interval">> := 10000}},
        api_get([message_queues, urlencode(<<"t/1">>)])
    ),
    ?assertMatch(
        {ok, 204},
        api_delete([message_queues, urlencode(<<"t/2">>)])
    ),
    ?assertMatch(
        {ok, 204},
        api_delete([message_queues, urlencode(<<"t/1">>)])
    ),
    ?assertMatch(
        {ok, 200, []},
        api_get([message_queues])
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

api_get(Path) ->
    R = request(get, uri(Path)),
    decode_body(R).

api_post(Path, Data) ->
    decode_body(request(post, uri(Path), Data)).

api_put(Path, Data) ->
    decode_body(request(put, uri(Path), Data)).

api_delete(Path) ->
    decode_body(request(delete, uri(Path))).

decode_body(Response) ->
    do_decode_body(Response).

do_decode_body({ok, Code, <<>>}) ->
    {ok, Code};
do_decode_body({ok, Code, Body}) ->
    case emqx_utils_json:safe_decode(Body) of
        {ok, Decoded} ->
            {ok, Code, Decoded};
        {error, _} = Error ->
            ct:pal("Invalid body: ~p", [Body]),
            Error
    end;
do_decode_body(Error) ->
    Error.

urlencode(X) when is_list(X) ->
    uri_string:quote(X);
urlencode(X) when is_binary(X) ->
    urlencode(binary_to_list(X)).
