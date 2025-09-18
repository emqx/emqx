%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
            ConnResId = <<"connector:http:a">>,
            ConnConfig = #{
                request_base => #{
                    scheme => http,
                    host => "localhost",
                    port => 18083
                },
                connect_timeout => 1000,
                pool_type => random,
                pool_size => 1,
                request => #{method => get}
            },
            {ok, ConnState0} = emqx_bridge_http_connector:on_start(
                ConnResId, ConnConfig
            ),
            ActionResId = <<"action:http:a:", ConnResId/binary>>,
            ActionConfig = #{
                parameters => #{
                    method => get,
                    path => "/status",
                    headers => auth_headers()
                },
                resource_opts => #{request_ttl => 5_000}
            },
            {ok, ConnState} = emqx_bridge_http_connector:on_add_channel(
                ConnResId, ConnState0, ActionResId, ActionConfig
            ),
            #{installed_actions := #{ActionResId := #{headers := Headers}}} = ConnState,
            {ok, 200, Req} = emqx_bridge_http_connector:on_query(
                foo, {ActionResId, #{}}, ConnState
            ),
            Tests =
                [
                    ?_assert(is_wrapped(V))
                 || H <- Headers, is_tuple({K, V} = H), is_auth_header(untmpl(K))
                ],
            [
                ?_assertEqual(4, length(Tests)),
                ?_assert(is_unwrapped_headers(element(2, Req)))
                | Tests
            ]
        end}.

auth_headers() ->
    [
        {<<"Authorization">>, ?MY_SECRET},
        {<<"authorization">>, ?MY_SECRET},
        {<<"Proxy-Authorization">>, ?MY_SECRET},
        {<<"proxy-authorization">>, ?MY_SECRET},
        {<<"X-Custom-Header">>, <<"foobar">>}
    ].

is_auth_header(<<"Authorization">>) -> true;
is_auth_header(<<"Proxy-Authorization">>) -> true;
is_auth_header(<<"authorization">>) -> true;
is_auth_header(<<"proxy-authorization">>) -> true;
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
