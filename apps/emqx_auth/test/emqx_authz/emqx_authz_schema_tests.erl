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

-module(emqx_authz_schema_tests).

-include_lib("eunit/include/eunit.hrl").

bad_authz_type_test() ->
    Txt = "[{type: foobar}]",
    ?assertThrow(
        [
            #{
                reason := "unknown_authz_type",
                got := <<"foobar">>
            }
        ],
        check(Txt)
    ).

bad_mongodb_type_test() ->
    Txt = "[{type: mongodb, mongo_type: foobar}]",
    ?assertThrow(
        [
            #{
                reason := "unknown_mongo_type",
                got := <<"foobar">>
            }
        ],
        check(Txt)
    ).

missing_mongodb_type_test() ->
    Txt = "[{type: mongodb}]",
    ?assertThrow(
        [
            #{
                reason := "unknown_mongo_type",
                got := undefined
            }
        ],
        check(Txt)
    ).

unknown_redis_type_test() ->
    Txt = "[{type: redis, redis_type: foobar}]",
    ?assertThrow(
        [
            #{
                reason := "unknown_redis_type",
                got := <<"foobar">>
            }
        ],
        check(Txt)
    ).

missing_redis_type_test() ->
    Txt = "[{type: redis}]",
    ?assertThrow(
        [
            #{
                reason := "unknown_redis_type",
                got := undefined
            }
        ],
        check(Txt)
    ).

unknown_http_method_test() ->
    Txt = "[{type: http, method: getx}]",
    ?assertThrow(
        [
            #{
                reason := "unknown_http_method",
                got := <<"getx">>
            }
        ],
        check(Txt)
    ).

missing_http_method_test() ->
    Txt = "[{type: http, methodx: get}]",
    ?assertThrow(
        [
            #{
                reason := "unknown_http_method",
                got := undefined
            }
        ],
        check(Txt)
    ).

check(Txt0) ->
    Txt = ["sources: ", Txt0],
    {ok, RawConf} = hocon:binary(Txt),
    try
        hocon_tconf:check_plain(schema(), RawConf, #{})
    catch
        throw:{_Schema, Errors} ->
            throw(Errors)
    end.

schema() ->
    #{roots => emqx_authz_schema:authz_fields()}.
