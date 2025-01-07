%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
