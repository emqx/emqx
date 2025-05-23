%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_LDAP_HRL).
-define(EMQX_LDAP_HRL, true).

-record(ldap_search_filter, {
    filter :: tuple()
}).

-record(ldap_dn, {
    dn :: list()
}).

-endif.
