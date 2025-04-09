%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_sup).
-feature(maybe_expr, enable).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Loader = fun emqx_license:read_license/0,
    maybe
        ok ?= validate_license(Loader),
        {ok,
            {
                #{
                    strategy => one_for_one,
                    intensity => 2,
                    period => 10
                },
                [
                    #{
                        id => license_checker,
                        start => {emqx_license_checker, start_link, [Loader]},
                        restart => permanent,
                        shutdown => 5000,
                        type => worker,
                        modules => [emqx_license_checker]
                    },

                    #{
                        id => license_resources,
                        start => {emqx_license_resources, start_link, []},
                        restart => permanent,
                        shutdown => 5000,
                        type => worker,
                        modules => [emqx_license_resources]
                    }
                ]
            }}
    end.

%% License violation check is done here (but not before boot)
%% because we must allow default single-node license to join cluster,
%% then check if the **fetched** license from peer node allows clustering.
validate_license(Loader) ->
    maybe
        {ok, License} ?= Loader(),
        ok ?= emqx_license_checker:no_violation(License)
    end.
