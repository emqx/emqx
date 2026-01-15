%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_AUTH_MNESIA_INTERNAL_HRL).
-define(EMQX_AUTH_MNESIA_INTERNAL_HRL, true).

%% Authn
-define(AUTHN_NS_TAB, emqx_authn_mnesia_ns).
-define(AUTHN_NS_KEY(NS, GROUP, ID), {NS, GROUP, ID}).

-record(?AUTHN_NS_TAB, {
    user_id :: ?AUTHN_NS_KEY(
        emqx_config:namespace(), emqx_authn_mnesia:user_group(), emqx_authn_mnesia:user_id()
    ),
    password_hash :: binary(),
    salt :: binary(),
    is_superuser :: boolean(),
    extra = #{} :: map()
}).

%% ETS table, only for counting records per namespace.
-define(AUTHN_NS_COUNT_TAB, emqx_authn_mnesia_ns_count).

%% Authz
-define(AUTHZ_NS_TAB, emqx_acl_ns).

-define(AUTHZ_WHO_NS(NS, WHO), {NS, WHO}).

-record(?AUTHZ_NS_TAB, {
    %% ?WHO_NS(emqx_config:namespace(), table_who())
    who,
    %% rules()
    rules,
    extra = #{}
}).

%% ETS table, only for counting records per namespace.
-define(AUTHZ_NS_COUNT_TAB, emqx_authz_mnesia_ns_count).

-endif.
