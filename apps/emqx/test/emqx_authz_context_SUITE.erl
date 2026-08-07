%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_context_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

t_security_boundary(_) ->
    ClientInfo = #{
        password => <<"passwd">>,
        custom_authz_field => custom_value
    },
    emqx_common_test_helpers:with_security_profile("legacy", fun() ->
        ?assertEqual(ClientInfo, emqx_authz_context:make(ClientInfo))
    end),
    emqx_common_test_helpers:with_security_profile("hardened", fun() ->
        Context = emqx_authz_context:make(ClientInfo),
        ?assertNot(maps:is_key(password, Context)),
        ?assertNot(maps:is_key(custom_authz_field, Context))
    end).
