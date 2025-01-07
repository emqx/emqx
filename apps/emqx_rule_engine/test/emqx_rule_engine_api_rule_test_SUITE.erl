%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rule_engine_api_rule_test_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_rule_engine,
            emqx_modules
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

t_ctx_pub(_) ->
    SQL = <<"SELECT payload.msg as msg, clientid, username, payload, topic, qos FROM \"t/#\"">>,
    Context = #{
        clientid => <<"c_emqx">>,
        event_type => message_publish,
        payload => <<"{\"msg\": \"hello\"}">>,
        qos => 1,
        topic => <<"t/a">>,
        username => <<"u_emqx">>
    },
    Expected = Context#{msg => <<"hello">>},
    do_test(SQL, Context, Expected).

t_ctx_sub(_) ->
    SQL = <<"SELECT clientid, username, topic, qos FROM \"$events/session_subscribed\"">>,
    Context = #{
        clientid => <<"c_emqx">>,
        event_type => session_subscribed,
        qos => 1,
        topic => <<"t/a">>,
        username => <<"u_emqx">>
    },

    do_test(SQL, Context, Context).

t_ctx_unsub(_) ->
    SQL = <<"SELECT clientid, username, topic, qos FROM \"$events/session_unsubscribed\"">>,
    Context = #{
        clientid => <<"c_emqx">>,
        event_type => session_unsubscribed,
        qos => 1,
        topic => <<"t/a">>,
        username => <<"u_emqx">>
    },
    do_test(SQL, Context, Context).

t_ctx_delivered(_) ->
    SQL =
        <<"SELECT from_clientid, from_username, topic, qos, node, timestamp FROM \"$events/message_delivered\"">>,
    Context = #{
        clientid => <<"c_emqx_2">>,
        event_type => message_delivered,
        from_clientid => <<"c_emqx_1">>,
        from_username => <<"u_emqx_1">>,
        payload => <<"{\"msg\": \"hello\"}">>,
        qos => 1,
        topic => <<"t/a">>,
        username => <<"u_emqx_2">>
    },
    Expected = check_result([from_clientid, from_username, topic, qos], [node, timestamp], Context),
    do_test(SQL, Context, Expected).

t_ctx_acked(_) ->
    SQL =
        <<"SELECT from_clientid, from_username, topic, qos, node, timestamp FROM \"$events/message_acked\"">>,

    Context = #{
        clientid => <<"c_emqx_2">>,
        event_type => message_acked,
        from_clientid => <<"c_emqx_1">>,
        from_username => <<"u_emqx_1">>,
        payload => <<"{\"msg\": \"hello\"}">>,
        qos => 1,
        topic => <<"t/a">>,
        username => <<"u_emqx_2">>
    },

    Expected = with_node_timestampe([from_clientid, from_username, topic, qos], Context),

    do_test(SQL, Context, Expected).

t_ctx_droped(_) ->
    SQL = <<"SELECT reason, topic, qos, node, timestamp FROM \"$events/message_dropped\"">>,
    Topic = <<"t/a">>,
    QoS = 1,
    Reason = <<"no_subscribers">>,
    Context = #{
        clientid => <<"c_emqx">>,
        event_type => message_dropped,
        payload => <<"{\"msg\": \"hello\"}">>,
        qos => QoS,
        reason => Reason,
        topic => Topic,
        username => <<"u_emqx">>
    },

    Expected = with_node_timestampe([reason, topic, qos], Context),
    do_test(SQL, Context, Expected).

t_ctx_connected(_) ->
    SQL =
        <<"SELECT clientid, username, keepalive, is_bridge FROM \"$events/client_connected\"">>,

    Context =
        #{
            clean_start => true,
            clientid => <<"c_emqx">>,
            event_type => client_connected,
            is_bridge => false,
            peername => <<"127.0.0.1:52918">>,
            username => <<"u_emqx">>
        },
    Expected = check_result([clientid, username, keepalive, is_bridge], [], Context),
    do_test(SQL, Context, Expected).

t_ctx_disconnected(_) ->
    SQL =
        <<"SELECT clientid, username, reason, disconnected_at, node FROM \"$events/client_disconnected\"">>,

    Context =
        #{
            clientid => <<"c_emqx">>,
            event_type => client_disconnected,
            reason => <<"normal">>,
            username => <<"u_emqx">>
        },
    Expected = check_result([clientid, username, reason], [disconnected_at, node], Context),
    do_test(SQL, Context, Expected).

t_ctx_connack(_) ->
    SQL =
        <<"SELECT clientid, username, reason_code, node FROM \"$events/client_connack\"">>,

    Context =
        #{
            clean_start => true,
            clientid => <<"c_emqx">>,
            event_type => client_connack,
            reason_code => <<"success">>,
            username => <<"u_emqx">>
        },
    Expected = check_result([clientid, username, reason_code], [node], Context),
    do_test(SQL, Context, Expected).

t_ctx_check_authz_complete(_) ->
    SQL =
        <<
            "SELECT clientid, username, topic, action, result,\n"
            "authz_source, node FROM \"$events/client_check_authz_complete\""
        >>,

    Context =
        #{
            action => <<"publish">>,
            clientid => <<"c_emqx">>,
            event_type => client_check_authz_complete,
            result => <<"allow">>,
            topic => <<"t/1">>,
            username => <<"u_emqx">>
        },
    Expected = check_result(
        [clientid, username, topic, action],
        [authz_source, node, result],
        Context
    ),

    do_test(SQL, Context, Expected).

t_ctx_check_authn_complete(_) ->
    SQL =
        <<
            "SELECT clientid, username, is_superuser, is_anonymous\n"
            "FROM \"$events/client_check_authn_complete\""
        >>,

    Context =
        #{
            clientid => <<"c_emqx">>,
            event_type => client_check_authn_complete,
            reason_code => <<"success">>,
            is_superuser => true,
            is_anonymous => false
        },
    Expected = check_result(
        [clientid, username, is_superuser, is_anonymous],
        [],
        Context
    ),

    do_test(SQL, Context, Expected).

t_ctx_delivery_dropped(_) ->
    SQL =
        <<"SELECT from_clientid, from_username, reason, topic, qos FROM \"$events/delivery_dropped\"">>,

    Context =
        #{
            clientid => <<"c_emqx_2">>,
            event_type => delivery_dropped,
            from_clientid => <<"c_emqx_1">>,
            from_username => <<"u_emqx_1">>,
            payload => <<"{\"msg\": \"hello\"}">>,
            qos => 1,
            reason => <<"queue_full">>,
            topic => <<"t/a">>,
            username => <<"u_emqx_2">>
        },
    Expected = check_result([from_clientid, from_username, reason, qos, topic], [], Context),
    do_test(SQL, Context, Expected).

t_ctx_schema_validation_failed(_) ->
    SQL =
        <<"SELECT validation FROM \"$events/schema_validation_failed\"">>,
    Context = #{
        <<"clientid">> => <<"c_emqx">>,
        <<"event_type">> => <<"schema_validation_failed">>,
        <<"payload">> => <<"{\"msg\": \"hello\"}">>,
        <<"qos">> => 1,
        <<"topic">> => <<"t/a">>,
        <<"username">> => <<"u_emqx">>,
        <<"validation">> => <<"m">>
    },
    Expected = check_result([validation], [], Context),
    do_test(SQL, Context, Expected).

t_ctx_message_transformation_failed(_) ->
    SQL =
        <<"SELECT transformation FROM \"$events/message_transformation_failed\"">>,
    Context = #{
        <<"clientid">> => <<"c_emqx">>,
        <<"event_type">> => <<"message_transformation_failed">>,
        <<"payload">> => <<"{\"msg\": \"hello\"}">>,
        <<"qos">> => 1,
        <<"topic">> => <<"t/a">>,
        <<"username">> => <<"u_emqx">>,
        <<"transformation">> => <<"m">>
    },
    Expected = check_result([transformation], [], Context),
    do_test(SQL, Context, Expected).

t_mongo_date_function_should_return_string_in_test_env(_) ->
    SQL =
        <<"SELECT mongo_date() as mongo_date FROM \"$events/client_check_authz_complete\"">>,
    Context =
        #{
            action => <<"publish">>,
            clientid => <<"c_emqx">>,
            event_type => client_check_authz_complete,
            result => <<"allow">>,
            topic => <<"t/1">>,
            username => <<"u_emqx">>
        },
    CheckFunction = fun(Result) ->
        MongoDate = maps:get(mongo_date, Result),
        %% Use regex to match the expected string
        MatchResult = re:run(MongoDate, <<"ISODate\\([0-9]{4}-[0-9]{2}-[0-9]{2}T.*\\)">>),
        ?assertMatch({match, _}, MatchResult),
        ok
    end,
    do_test(SQL, Context, CheckFunction).

do_test(SQL, Context, Expected0) ->
    Res = emqx_rule_engine_api:'/rule_test'(
        post,
        test_rule_params(SQL, Context)
    ),
    ?assertMatch({200, _}, Res),
    {200, Result0} = Res,
    Result = emqx_utils_maps:unsafe_atom_key_map(Result0),
    case is_function(Expected0) of
        false ->
            Expected = maps:without([event_type], Expected0),
            ?assertMatch(Expected, Result, Expected);
        _ ->
            Expected0(Result)
    end,
    ok.

test_rule_params(Sql, Context) ->
    #{
        body => #{
            <<"context">> => Context,
            <<"sql">> => Sql
        }
    }.

with_node_timestampe(Keys, Context) ->
    check_result(Keys, [node, timestamp], Context).

check_result(Keys, Exists, Context) ->
    Log = fun(Format, Args) ->
        lists:flatten(io_lib:format(Format, Args))
    end,

    Base = maps:with(Keys, Context),

    fun(Result) ->
        maps:foreach(
            fun(Key, Value) ->
                ?assertEqual(
                    Value,
                    maps:get(Key, Result, undefined),
                    Log("Key:~p value error~nResult:~p~n", [Key, Result])
                )
            end,
            Base
        ),

        NotExists = fun(Key) -> Log("Key:~p not exists in result:~p~n", [Key, Result]) end,
        lists:foreach(
            fun(Key) ->
                Find = maps:find(Key, Result),
                Formatter = NotExists(Key),
                ?assertMatch({ok, _}, Find, Formatter),
                ?assertNotMatch({ok, undefined}, Find, Formatter),
                ?assertNotMatch({ok, <<"undefined">>}, Find, Formatter)
            end,
            Exists
        ),

        ?assertEqual(erlang:length(Keys) + erlang:length(Exists), maps:size(Result), Result)
    end.
