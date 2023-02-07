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

-module(emqx_authn_schema_tests).

-include_lib("eunit/include/eunit.hrl").

%% schema error
-define(ERR(Reason), {error, Reason}).

union_member_selector_mongo_test_() ->
    Check = fun(Txt) -> check(emqx_authn_mongodb, Txt) end,
    [
        {"unknown", fun() ->
            ?assertMatch(
                ?ERR(#{field_name := mongo_type, expected := _}),
                Check("{mongo_type: foobar}")
            )
        end},
        {"single", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn-mongodb:standalone"}),
                Check("{mongo_type: single}")
            )
        end},
        {"replica-set", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn-mongodb:replica-set"}),
                Check("{mongo_type: rs}")
            )
        end},
        {"sharded", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn-mongodb:sharded-cluster"}),
                Check("{mongo_type: sharded}")
            )
        end}
    ].

union_member_selector_jwt_test_() ->
    Check = fun(Txt) -> check(emqx_authn_jwt, Txt) end,
    [
        {"unknown", fun() ->
            ?assertMatch(
                ?ERR(#{field_name := use_jwks, expected := "true | false"}),
                Check("{use_jwks = 1}")
            )
        end},
        {"jwks", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn-jwt:jwks"}),
                Check("{use_jwks = true}")
            )
        end},
        {"publick-key", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn-jwt:public-key"}),
                Check("{use_jwks = false, public_key = 1}")
            )
        end},
        {"hmac-based", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn-jwt:hmac-based"}),
                Check("{use_jwks = false}")
            )
        end}
    ].

union_member_selector_redis_test_() ->
    Check = fun(Txt) -> check(emqx_authn_redis, Txt) end,
    [
        {"unknown", fun() ->
            ?assertMatch(
                ?ERR(#{field_name := redis_type, expected := _}),
                Check("{redis_type = 1}")
            )
        end},
        {"single", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn-redis:standalone"}),
                Check("{redis_type = single}")
            )
        end},
        {"cluster", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn-redis:cluster"}),
                Check("{redis_type = cluster}")
            )
        end},
        {"sentinel", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn-redis:sentinel"}),
                Check("{redis_type = sentinel}")
            )
        end}
    ].

union_member_selector_http_test_() ->
    Check = fun(Txt) -> check(emqx_authn_http, Txt) end,
    [
        {"unknown", fun() ->
            ?assertMatch(
                ?ERR(#{field_name := method, expected := _}),
                Check("{method = 1}")
            )
        end},
        {"get", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn-http:get"}),
                Check("{method = get}")
            )
        end},
        {"post", fun() ->
            ?assertMatch(
                ?ERR(#{matched_type := "authn-http:post"}),
                Check("{method = post}")
            )
        end}
    ].

check(Module, HoconConf) ->
    emqx_hocon:check(Module, ["authentication= ", HoconConf]).
