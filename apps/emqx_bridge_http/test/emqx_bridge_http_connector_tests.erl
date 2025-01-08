%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(emqx_bridge_http_connector_tests).

-include_lib("eunit/include/eunit.hrl").

-define(MY_SECRET, <<"my_precious">>).

wrap_auth_headers_test_() ->
    {setup,
        fun() ->
            meck:expect(ehttpc_sup, start_pool, 2, {ok, foo}),
            meck:expect(ehttpc, request, fun(_, _, Req, _, _) -> {ok, 200, Req} end),
            meck:expect(ehttpc, workers, 1, [{self, self()}]),
            meck:expect(ehttpc, health_check, 2, ok),
            meck:expect(ehttpc_pool, pick_worker, 1, self()),
            meck:expect(emqx_resource, allocate_resource, 3, ok),
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
    Conf0 = parse(webhook_config_hocon()),
    ?assertMatch(
        #{<<"method">> := _},
        emqx_utils_maps:deep_get([<<"bridges">>, <<"webhook">>, <<"a">>], Conf0)
    ),
    lists:foreach(
        fun(Method) ->
            Conf1 = emqx_utils_maps:deep_put(
                [<<"bridges">>, <<"webhook">>, <<"a">>, <<"method">>],
                Conf0,
                Method
            ),
            ?assertMatch(
                #{},
                check(Conf1),
                #{method => Method}
            ),
            ?assertMatch(
                #{},
                check_atom_key(Conf1),
                #{method => Method}
            ),
            ok
        end,
        [<<"post">>, <<"put">>, <<"get">>, <<"delete">>]
    ),
    lists:foreach(
        fun(Method) ->
            Conf1 = emqx_utils_maps:deep_put(
                [<<"bridges">>, <<"webhook">>, <<"a">>, <<"method">>],
                Conf0,
                Method
            ),
            ?assertThrow(
                {_, [
                    #{
                        kind := validation_error,
                        reason := not_a_enum_symbol
                    }
                ]},
                check(Conf1),
                #{method => Method}
            ),
            ?assertThrow(
                {_, [
                    #{
                        kind := validation_error,
                        reason := not_a_enum_symbol
                    }
                ]},
                check_atom_key(Conf1),
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

parse(Hocon) ->
    {ok, Conf} = hocon:binary(Hocon),
    Conf.

%% what bridge creation does
check(Conf) when is_map(Conf) ->
    hocon_tconf:check_plain(emqx_bridge_schema, Conf).

%% what bridge probe does
check_atom_key(Conf) when is_map(Conf) ->
    hocon_tconf:check_plain(emqx_bridge_schema, Conf, #{atom_key => true, required => false}).

%%===========================================================================
%% Data section
%%===========================================================================

%% erlfmt-ignore
webhook_config_hocon() ->
"
bridges.webhook.a {
  body = \"${.}\"
  connect_timeout = 15s
  enable = false
  enable_pipelining = 100
  headers {content-type = \"application/json\", jjjjjjjjjjjjjjjjjjj = jjjjjjj}
  max_retries = 2
  method = post
  pool_size = 8
  pool_type = random
  resource_opts {
    health_check_interval = 15s
    inflight_window = 100
    max_buffer_bytes = 1GB
    query_mode = async
    request_ttl = 45s
    start_after_created = true
    start_timeout = 5s
    worker_pool_size = 4
  }
  ssl {
    ciphers = []
    depth = 10
    enable = false
    hibernate_after = 5s
    log_level = notice
    reuse_sessions = true
    secure_renegotiate = true
    verify = verify_peer
    versions = [tlsv1.3, tlsv1.2]
  }
  url = \"http://some.host:4000/api/echo\"
}
".
