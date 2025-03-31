%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_test_mod).

%% Authorization callbacks
-export([
    init/1,
    authorize/2,
    description/0
]).

init(AuthzOpts) ->
    {ok, AuthzOpts}.

authorize({_User, _PubSub, _Topic}, _State) ->
    allow.

description() ->
    "Test Authorization Mod".
