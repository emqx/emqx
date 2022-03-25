%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% @doc EMQX License Management Supervisor.
%%--------------------------------------------------------------------

-module(emqx_license_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok,
        {
            #{
                strategy => one_for_one,
                intensity => 10,
                period => 100
            },

            [
                #{
                    id => license_checker,
                    start => {emqx_license_checker, start_link, [fun emqx_license:read_license/0]},
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
                },

                #{
                    id => license_installer,
                    start => {emqx_license_installer, start_link, [fun emqx_license:load/0]},
                    restart => permanent,
                    shutdown => 5000,
                    type => worker,
                    modules => [emqx_license_installer]
                }
            ]
        }}.
