%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_sup).

-behaviour(supervisor).

-export([
    start_link/0,
    init/1
]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 100,
        period => 10
    },

    AuthN = #{
        id => emqx_authn_sup,
        start => {emqx_authn_sup, start_link, []},
        restart => permanent,
        shutdown => 1000,
        type => supervisor
    },

    AuthZ = #{
        id => emqx_authz_sup,
        start => {emqx_authz_sup, start_link, []},
        restart => permanent,
        shutdown => 1000,
        type => supervisor
    },

    ChildSpecs = [AuthN, AuthZ],

    {ok, {SupFlags, ChildSpecs}}.
