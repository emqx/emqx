%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cluster_link_tests).

-include_lib("eunit/include/eunit.hrl").

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

bin(X) -> emqx_utils_conv:bin(X).

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

link_params(Name) ->
    link_params(Name, _Overrides = #{}).

link_params(Name, Overrides) ->
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

schema_test_() ->
    [
        {"topic filters must be unique",
            ?_assertThrow(
                {_Schema, [
                    #{
                        reason := #{
                            reason := invalid_topics,
                            topics := [{<<"t">>, duplicate_topic_filter}]
                        },
                        value := [_, _],
                        kind := validation_error
                    }
                ]},
                parse_and_check([
                    link_params(<<"l1">>, #{<<"topics">> => [<<"t">>, <<"t">>]})
                ])
            )}
    ].
