%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cluster_link_schema_tests).

-include_lib("eunit/include/eunit.hrl").

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

parse_and_check(InnerConfigs) ->
    RootBin = <<"links">>,
    RawConf = #{RootBin => InnerConfigs},
    #{RootBin := Checked} = hocon_tconf:check_plain(
        #{roots => [{links, emqx_cluster_link_schema:links_schema(#{})}]},
        RawConf,
        #{
            required => false,
            atom_key => false,
            make_serializable => false
        }
    ),
    Checked.

link(Name, Overrides) ->
    Default = #{
        <<"name">> => Name,
        <<"clientid">> => <<"linkclientid">>,
        <<"password">> => <<"my secret password">>,
        <<"pool_size">> => 1,
        <<"server">> => <<"emqxcl_2.nohost:31883">>,
        <<"topics">> => [<<"t/test-topic">>, <<"t/test/#">>]
    },
    emqx_utils_maps:deep_merge(Default, Overrides).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

invalid_topics_test_() ->
    {"Invalid topics are rejected",
        ?_assertThrow(
            {_Schema, [
                #{
                    kind := validation_error,
                    reason := #{
                        reason := invalid_topics,
                        topics := [{<<>>, empty_topic}, {<<"$SYS/#/+">>, 'topic_invalid_#'}]
                    }
                }
            ]},
            parse_and_check([
                link(<<"link1">>, #{
                    <<"topics">> => [<<"t/+">>, <<>>, <<"$SYS/#/+">>]
                })
            ])
        )}.

redundant_topics_test_() ->
    {"Redundant topics is an error",
        ?_assertThrow(
            {_Schema, [
                #{
                    kind := validation_error,
                    reason := #{
                        reason := redundant_topics,
                        topics := [<<"t/+">>, <<"t/1">>]
                    }
                }
            ]},
            parse_and_check([
                link(<<"link1">>, #{
                    <<"topics">> => [<<"t/+">>, <<"g/2">>, <<"t/1">>, <<"t/+/#">>, <<"t">>]
                })
            ])
        )}.

special_topics_test_() ->
    {"$LINK topics are not allowed",
        ?_assertThrow(
            {_Schema, [
                #{
                    kind := validation_error,
                    reason := #{
                        reason := invalid_topics,
                        topics := [{<<"$LINK/cluster/+/name">>, topic_not_allowed}]
                    }
                }
            ]},
            parse_and_check([
                link(<<"link1">>, #{
                    <<"topics">> => [<<"t/+">>, <<"t/1">>, <<"$LINK/cluster/+/name">>]
                })
            ])
        )}.

tcp_opts_schema_test_() ->
    {"tcp_opts: schema parses and is forwarded to emqtt options",
        ?_test(begin
            [#{<<"tcp_opts">> := TcpOpts}] = parse_and_check([
                link(<<"link1">>, #{
                    <<"tcp_opts">> => #{
                        <<"nodelay">> => true,
                        <<"sndbuf">> => <<"16KB">>,
                        <<"recbuf">> => <<"8KB">>,
                        <<"buffer">> => <<"32KB">>,
                        <<"keepalive">> => false
                    }
                })
            ]),
            ?assertMatch(
                #{
                    <<"nodelay">> := true,
                    <<"sndbuf">> := 16384,
                    <<"recbuf">> := 8192,
                    <<"buffer">> := 32768,
                    <<"keepalive">> := false
                },
                TcpOpts
            ),
            with_cluster_name(fun() ->
                LinkConf = #{
                    server => <<"127.0.0.1:1883">>,
                    clientid => <<"linkclientid">>,
                    ssl => #{enable => false},
                    tcp_opts => #{
                        nodelay => true,
                        sndbuf => 16384,
                        recbuf => 8192,
                        buffer => 32768,
                        keepalive => false
                    }
                },
                #{tcp_opts := Proplist} = emqx_cluster_link_config:mk_emqtt_options(LinkConf),
                ?assertEqual(true, proplists:get_value(nodelay, Proplist)),
                ?assertEqual(16384, proplists:get_value(sndbuf, Proplist)),
                ?assertEqual(8192, proplists:get_value(recbuf, Proplist)),
                ?assertEqual(32768, proplists:get_value(buffer, Proplist)),
                ?assertEqual(false, proplists:get_value(keepalive, Proplist))
            end)
        end)}.

tcp_opts_default_test_() ->
    {"tcp_opts: when unset, mk_emqtt_options forwards an empty proplist",
        ?_test(
            with_cluster_name(fun() ->
                LinkConf = #{
                    server => <<"127.0.0.1:1883">>,
                    clientid => <<"linkclientid">>,
                    ssl => #{enable => false}
                },
                #{tcp_opts := Proplist} = emqx_cluster_link_config:mk_emqtt_options(LinkConf),
                ?assertEqual([], Proplist)
            end)
        )}.

with_cluster_name(Fun) ->
    %% `emqx_cluster_link_config:mk_emqtt_options/1' falls back on `cluster()' as a
    %% default value for clientid; that lookup needs `[cluster, name]' to exist in
    %% `emqx_config'.  Stub it with a dummy value for the duration of the test.
    Prior =
        try
            {ok, emqx_config:get([cluster, name])}
        catch
            _:_ -> undefined
        end,
    emqx_config:put([cluster, name], 'test@nohost'),
    try
        Fun()
    after
        case Prior of
            {ok, V} -> emqx_config:put([cluster, name], V);
            _ -> ok
        end
    end.
