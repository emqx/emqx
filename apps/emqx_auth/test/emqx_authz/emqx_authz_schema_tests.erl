%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
