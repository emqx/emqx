%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_greptimedb_tests).

-include_lib("eunit/include/eunit.hrl").

%% https://github.com/emqx/emqx/issues/19814
missing_write_syntax_test() ->
    ?assertThrow(
        {_, [#{reason := required_field, path := "parameters.write_syntax"}]},
        check_action_parameters(#{<<"precision">> => <<"ms">>})
    ).

check_action_parameters(Conf) ->
    Schema = #{
        roots => [{parameters, hoconsc:mk(hoconsc:ref(emqx_bridge_greptimedb, action_parameters))}]
    },
    hocon_tconf:check_plain(Schema, #{<<"parameters">> => Conf}, #{required => false}).
