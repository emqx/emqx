%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_connector_http_tests).

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
                base_url => #{
                    scheme => http,
                    host => "localhost",
                    port => 18083,
                    path => "/status"
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
            {ok, #{request := #{headers := Headers}} = State} = emqx_connector_http:on_start(
                <<"test">>, Config
            ),
            {ok, 200, Req} = emqx_connector_http:on_query(foo, {send_message, #{}}, State),
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

untmpl([{_, V} | _]) -> V.

is_unwrapped_headers(Headers) ->
    lists:all(fun is_unwrapped_header/1, Headers).

is_unwrapped_header({_, V}) when is_function(V) -> false;
is_unwrapped_header({_, [{str, _V}]}) -> throw(unexpected_tmpl_token);
is_unwrapped_header(_) -> true.
