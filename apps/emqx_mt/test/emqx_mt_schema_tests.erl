%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mt_schema_tests).

-include_lib("eunit/include/eunit.hrl").

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

parse_and_check(InnerConfigs) ->
    RootBin = <<"multi_tenancy">>,
    RawConf = #{RootBin => InnerConfigs},
    #{RootBin := Checked} = hocon_tconf:check_plain(
        emqx_mt_schema,
        RawConf,
        #{
            required => false,
            atom_key => false,
            make_serializable => false
        }
    ),
    Checked.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

default_max_sessions_positive_test() ->
    ?assertThrow(
        {_, [
            #{
                kind := validation_error,
                %% Wrong type
                reason := matched_no_union_member
            }
        ]},
        parse_and_check(#{<<"default_max_sessions">> => 0})
    ).
