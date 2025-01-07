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

-module(emqx_authn_schema_tests).

-include_lib("eunit/include/eunit.hrl").

%% schema error
-define(ERR(Reason), {error, Reason}).

union_member_selector_mongo_test_() ->
    ok = ensure_schema_load(),
    [
        {"unknown", fun() ->
            ?assertMatch(
                ?ERR(#{field_name := mongo_type, expected := _}),
                check("{mechanism = password_based, backend = mongodb, mongo_type = foobar}")
            )
        end},
        {"single", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn:mongo_single"}),
                check("{mechanism = password_based, backend = mongodb, mongo_type = single}")
            )
        end},
        {"replica-set", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn:mongo_rs"}),
                check("{mechanism = password_based, backend = mongodb, mongo_type = rs}")
            )
        end},
        {"sharded", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn:mongo_sharded"}),
                check("{mechanism = password_based, backend = mongodb, mongo_type = sharded}")
            )
        end}
    ].

union_member_selector_jwt_test_() ->
    ok = ensure_schema_load(),
    [
        {"unknown", fun() ->
            ?assertMatch(
                ?ERR(#{field_name := use_jwks, expected := "true | false"}),
                check("{mechanism = jwt, use_jwks = 1}")
            )
        end},
        {"jwks", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn:jwt_jwks"}),
                check("{mechanism = jwt, use_jwks = true}")
            )
        end},
        {"publick-key", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn:jwt_public_key"}),
                check("{mechanism = jwt, use_jwks = false, public_key = 1}")
            )
        end},
        {"hmac-based", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn:jwt_hmac"}),
                check("{mechanism = jwt, use_jwks = false}")
            )
        end}
    ].

union_member_selector_redis_test_() ->
    ok = ensure_schema_load(),
    [
        {"unknown", fun() ->
            ?assertMatch(
                ?ERR(#{field_name := redis_type, expected := _}),
                check("{mechanism = password_based, backend = redis, redis_type = 1}")
            )
        end},
        {"single", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn:redis_single"}),
                check("{mechanism = password_based, backend = redis, redis_type = single}")
            )
        end},
        {"cluster", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn:redis_cluster"}),
                check("{mechanism = password_based, backend = redis, redis_type = cluster}")
            )
        end},
        {"sentinel", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn:redis_sentinel"}),
                check("{mechanism = password_based, backend = redis, redis_type = sentinel}")
            )
        end}
    ].

union_member_selector_http_test_() ->
    ok = ensure_schema_load(),
    [
        {"unknown", fun() ->
            ?assertMatch(
                ?ERR(#{field_name := method, expected := _}),
                check("{mechanism = password_based, backend = http, method = 1}")
            )
        end},
        {"get", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn:http_get"}),
                check("{mechanism = password_based, backend = http, method = get}")
            )
        end},
        {"post", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn:http_post"}),
                check("{mechanism = password_based, backend = http, method = post}")
            )
        end}
    ].

check(HoconConf) ->
    emqx_hocon:check(
        #{roots => emqx_authn_schema:global_auth_fields()},
        ["authentication= ", HoconConf]
    ).

ensure_schema_load() ->
    _ = emqx_conf_schema:roots(),
    ok.
