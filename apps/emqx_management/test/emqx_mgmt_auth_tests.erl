%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mgmt_auth_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx_utils/include/emqx_api_key_scopes.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard_rbac.hrl").

bootstrap_scopes_drop_reasons_test() ->
    %% An administrator line mixing a privilege scope, a valid non-privilege
    %% scope and a typo: keep the non-privilege scope, drop the other two with
    %% distinct reasons.
    {AdminValid, AdminRejected} =
        emqx_mgmt_auth:parse_bootstrap_scopes_lenient(
            ?ROLE_API_SUPERUSER, <<"system,connections,bogus_scope">>
        ),
    ?assertEqual([?SCOPE_CONNECTIONS], AdminValid),
    ?assertEqual(
        #{
            privilege_scope_conflict => [?SCOPE_SYSTEM],
            unknown_scope => [<<"bogus_scope">>]
        },
        emqx_mgmt_auth:group_rejected_by_reason(AdminRejected)
    ),
    %% A publisher line: any non-`publish' scope is dropped with the
    %% publisher-role reason.
    {PubValid, PubRejected} =
        emqx_mgmt_auth:parse_bootstrap_scopes_lenient(
            ?ROLE_API_PUBLISHER, <<"publish,connections">>
        ),
    ?assertEqual([?SCOPE_PUBLISH], PubValid),
    ?assertEqual(
        #{not_allowed_for_publisher_role => [?SCOPE_CONNECTIONS]},
        emqx_mgmt_auth:group_rejected_by_reason(PubRejected)
    ).
