%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module implelements the hook callback for multi-tenancy.
-module(emqx_mt_hookcb).

-export([
    register_hooks/0,
    on_session_created/2
]).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

register_hooks() ->
    MFA = {?MODULE, on_session_created, []},
    ok = emqx_hooks:add('session.created', MFA, ?HP_SYS_MSGS),
    ok.

on_session_created(
    #{
        clientid := ClientId,
        client_attrs := #{?CLIENT_ATTR_NAME_TNS := Tns}
    },
    _SessionInfo
) ->
    ok = emqx_mt_pool:add(Tns, ClientId, self());
on_session_created(_ClientInfo, _SessionInfo) ->
    %% not a multi-tenant client
    ok.
