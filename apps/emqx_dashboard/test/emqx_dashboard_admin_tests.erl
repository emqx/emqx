%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_admin_tests).

-include_lib("eunit/include/eunit.hrl").

format_username_test_() ->
    [
        {"plain binary username", fun() ->
            ?assertEqual(<<"admin">>, emqx_dashboard_admin:format_username(<<"admin">>))
        end},
        {"SSO username tuple", fun() ->
            ?assertEqual(
                <<"oidc:alice">>,
                emqx_dashboard_admin:format_username({oidc, <<"alice">>})
            )
        end},
        {"bare backend atom, pre-authentication log_source", fun() ->
            ?assertEqual(<<"oidc">>, emqx_dashboard_admin:format_username(oidc))
        end}
    ].
