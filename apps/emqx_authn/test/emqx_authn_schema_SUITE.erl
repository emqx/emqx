-module(emqx_authn_schema_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-include("emqx_authn.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    emqx_common_test_helpers:start_apps([emqx_authn]),
    Config.

end_per_suite(_) ->
    emqx_common_test_helpers:stop_apps([emqx_authn]),
    ok.

init_per_testcase(_Case, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    mria:clear_table(emqx_authn_mnesia),
    Config.

end_per_testcase(_Case, Config) ->
    Config.

-define(CONF(Conf), #{?CONF_NS_BINARY => Conf}).

t_check_schema(_Config) ->
    Check = fun(C) -> emqx_config:check_config(emqx_schema, ?CONF(C)) end,
    ConfigOk = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"built_in_database">>,
        <<"user_id_type">> => <<"username">>,
        <<"password_hash_algorithm">> => #{
            <<"name">> => <<"bcrypt">>,
            <<"salt_rounds">> => <<"6">>
        }
    },
    _ = Check(ConfigOk),

    ConfigNotOk = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"built_in_database">>,
        <<"user_id_type">> => <<"username">>,
        <<"password_hash_algorithm">> => #{
            <<"name">> => <<"md6">>
        }
    },
    ?assertThrow(
        #{
            path := "authentication.1.password_hash_algorithm.name",
            matched_type := "authn:builtin_db/authn-hash:simple",
            reason := unable_to_convert_to_enum_symbol
        },
        Check(ConfigNotOk)
    ),

    ConfigMissingAlgoName = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"built_in_database">>,
        <<"user_id_type">> => <<"username">>,
        <<"password_hash_algorithm">> => #{
            <<"foo">> => <<"bar">>
        }
    },

    ?assertThrow(
        #{
            path := "authentication.1.password_hash_algorithm",
            reason := "algorithm_name_missing",
            matched_type := "authn:builtin_db"
        },
        Check(ConfigMissingAlgoName)
    ).

t_union_member_selector(_) ->
    ?assertMatch(#{authentication := undefined}, check(undefined)),
    C1 = #{<<"backend">> => <<"built_in_database">>},
    ?assertThrow(
        #{
            path := "authentication.1",
            reason := "missing_mechanism_field"
        },
        check(C1)
    ),
    C2 = <<"foobar">>,
    ?assertThrow(
        #{
            path := "authentication.1",
            reason := "not_a_struct",
            value := <<"foobar">>
        },
        check(C2)
    ),
    Base = #{
        <<"user_id_type">> => <<"username">>,
        <<"password_hash_algorithm">> => #{
            <<"name">> => <<"plain">>
        }
    },
    BadBackend = Base#{<<"mechanism">> => <<"password_based">>, <<"backend">> => <<"bar">>},
    ?assertThrow(
        #{
            reason := "unknown_backend",
            backend := <<"bar">>
        },
        check(BadBackend)
    ),
    BadMechanism = Base#{<<"mechanism">> => <<"foo">>, <<"backend">> => <<"built_in_database">>},
    ?assertThrow(
        #{
            reason := "unsupported_mechanism",
            mechanism := <<"foo">>,
            backend := <<"built_in_database">>
        },
        check(BadMechanism)
    ),
    BadCombination = Base#{<<"mechanism">> => <<"scram">>, <<"backend">> => <<"http">>},
    ?assertThrow(
        #{
            reason := "unsupported_mechanism",
            mechanism := <<"scram">>,
            backend := <<"http">>
        },
        check(BadCombination)
    ),
    ok.

t_http_auth_selector(_) ->
    C1 = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"http">>
    },
    ?assertThrow(
        #{
            field_name := method,
            expected := "get | post"
        },
        check(C1)
    ),
    ok.

t_mongo_auth_selector(_) ->
    C1 = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"mongodb">>
    },
    ?assertThrow(
        #{
            field_name := mongo_type,
            expected := "single | rs | sharded"
        },
        check(C1)
    ),
    ok.

t_redis_auth_selector(_) ->
    C1 = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"redis">>
    },
    ?assertThrow(
        #{
            field_name := redis_type,
            expected := "single | cluster | sentinel"
        },
        check(C1)
    ),
    ok.

t_redis_jwt_selector(_) ->
    C1 = #{
        <<"mechanism">> => <<"jwt">>
    },
    ?assertThrow(
        #{
            field_name := use_jwks,
            expected := "true | false"
        },
        check(C1)
    ),
    ok.

check(C) ->
    {_Mappings, Checked} = emqx_config:check_config(emqx_schema, ?CONF(C)),
    Checked.
