%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_http_connector_tests).

-include_lib("eunit/include/eunit.hrl").

-define(MY_SECRET, <<"my_precious">>).

wrap_auth_headers_test_() ->
    {setup,
        fun() ->
            meck:expect(ehttpc_sup, start_pool, 2, {ok, foo}),
            meck:expect(ehttpc, check_pool_integrity, 1, ok),
            meck:expect(ehttpc, request, fun(_, _, Req, _, _) -> {ok, 200, Req} end),
            meck:expect(ehttpc, workers, 1, [{self, self()}]),
            meck:expect(ehttpc, health_check, 2, ok),
            meck:expect(ehttpc_pool, pick_worker, 1, self()),
            meck:expect(emqx_resource, allocate_resource, 4, ok),
            [ehttpc_sup, ehttpc, ehttpc_pool, emqx_resource]
        end,
        fun meck:unload/1, fun(_) ->
            Config = #{
                request_base => #{
                    scheme => http,
                    host => "localhost",
                    port => 18083
                },
                connect_timeout => 1000,
                pool_type => random,
                pool_size => 1,
                request => #{
                    method => get,
                    path => "/status",
                    headers => auth_headers()
                }
            },
            {ok, #{request := #{headers := Headers}} = State} = emqx_bridge_http_connector:on_start(
                <<"test">>, Config
            ),
            {ok, 200, Req} = emqx_bridge_http_connector:on_query(foo, {send_message, #{}}, State),
            WrappedTests =
                [
                    ?_assert(is_wrapped(V))
                 || H <- Headers, is_tuple({K, V} = H), is_auth_header(untmpl(K))
                ],
            UnwrappedTests =
                [
                    ?_assertNot(is_function(V))
                 || H <- Headers, is_tuple({K, V} = H), not is_auth_header(untmpl(K))
                ],
            [
                ?_assertEqual(9, length(WrappedTests)),
                ?_assertEqual(2, length(UnwrappedTests)),
                ?_assert(is_unwrapped_headers(element(2, Req)))
                | WrappedTests ++ UnwrappedTests
            ]
        end}.

auth_headers() ->
    [
        {<<"Authorization">>, ?MY_SECRET},
        {<<"authorization">>, ?MY_SECRET},
        {<<"Proxy-Authorization">>, ?MY_SECRET},
        {<<"proxy-authorization">>, ?MY_SECRET},
        {<<"x-api-key">>, ?MY_SECRET},
        {<<"X-Api-Key">>, ?MY_SECRET},
        {<<"X-API-KEY">>, ?MY_SECRET},
        {<<"cookie">>, ?MY_SECRET},
        {<<"Cookie">>, ?MY_SECRET},
        {<<"content-type">>, <<"application/json">>},
        {<<"accept">>, <<"application/json">>}
    ].

is_auth_header(<<"Authorization">>) -> true;
is_auth_header(<<"Proxy-Authorization">>) -> true;
is_auth_header(<<"authorization">>) -> true;
is_auth_header(<<"proxy-authorization">>) -> true;
is_auth_header(<<"x-api-key">>) -> true;
is_auth_header(<<"X-Api-Key">>) -> true;
is_auth_header(<<"X-API-KEY">>) -> true;
is_auth_header(<<"cookie">>) -> true;
is_auth_header(<<"Cookie">>) -> true;
is_auth_header(_Other) -> false.

is_wrapped(Secret) when is_function(Secret) ->
    untmpl(emqx_secret:unwrap(Secret)) =:= ?MY_SECRET;
is_wrapped(_Other) ->
    false.

untmpl(Tpl) ->
    iolist_to_binary(emqx_template:render_strict(Tpl, #{})).

is_unwrapped_headers(Headers) ->
    lists:all(fun is_unwrapped_header/1, Headers).

is_unwrapped_header({_, V}) when is_function(V) -> false;
is_unwrapped_header({_, [{str, _V}]}) -> throw(unexpected_tmpl_token);
is_unwrapped_header(_) -> true.

transform_result_drops_ehttpc_worker_down_call_args_test() ->
    LeakyHeaders = [
        {<<"authorization">>, <<"Bearer secret-authz-token">>},
        {<<"x-api-key">>, <<"secret-authz-key">>}
    ],
    Reason =
        {ehttpc_worker_down,
            {killed,
                {gen_server, call, [
                    self(), {get, {[[], 47, <<"authz">>], LeakyHeaders}, 1782800788278}, 5500
                ]}}},
    {error, Sanitized} = emqx_bridge_http_connector:transform_result({error, Reason}),
    Flat = lists:flatten(io_lib:format("~0p", [Sanitized])),
    ?assertEqual(0, string:str(Flat, "secret-authz-token"), Flat),
    ?assertEqual(0, string:str(Flat, "secret-authz-key"), Flat),
    %% Worker-down classification and stop reason are kept; the call args are
    %% dropped entirely.
    ?assertEqual({ehttpc_worker_down, {killed, {gen_server, call, '...'}}}, Sanitized),
    ok.

transform_result_drops_wrapped_ehttpc_worker_down_call_args_test() ->
    LeakyHeaders = [{<<"authorization">>, <<"Bearer secret-authz-token">>}],
    Reason =
        {ehttpc_worker_down,
            {killed,
                {gen_server, call, [
                    self(), {get, {[<<"authz">>], LeakyHeaders}, 1}, 5500
                ]}}},
    %% Arrives wrapped in `{shutdown, ...}`; the recursion must still sanitize it.
    {error, Sanitized} = emqx_bridge_http_connector:transform_result(
        {error, {shutdown, Reason}}
    ),
    Flat = lists:flatten(io_lib:format("~0p", [Sanitized])),
    ?assertEqual(0, string:str(Flat, "secret-authz-token"), Flat),
    ?assertEqual({ehttpc_worker_down, {killed, {gen_server, call, '...'}}}, Sanitized),
    ok.

method_validator_test() ->
    lists:foreach(
        fun(Method) ->
            ?assertMatch(
                #{},
                check_action(#{<<"parameters">> => #{<<"method">> => Method}}),
                #{method => Method}
            ),
            ok
        end,
        [<<"post">>, <<"put">>, <<"get">>, <<"delete">>]
    ),
    lists:foreach(
        fun(Method) ->
            ?assertThrow(
                {_, [
                    #{
                        kind := validation_error,
                        reason := not_a_enum_symbol
                    }
                ]},
                check_action(#{<<"parameters">> => #{<<"method">> => Method}}),
                #{method => Method}
            ),
            ok
        end,
        [<<"x">>, <<"patch">>, <<"options">>]
    ),
    ok.

%%===========================================================================
%% Helper functions
%%===========================================================================

check_action(Overrides) ->
    emqx_bridge_schema_testlib:http_action_config(Overrides#{<<"connector">> => <<"x">>}).

%%===========================================================================
%% Data section
%%===========================================================================
