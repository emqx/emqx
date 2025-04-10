%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_sup).

-behaviour(supervisor).

-export([start_link/1]).

-export([init/1]).

start_link(Reader) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Reader]).

init([Reader]) ->
    Strategy =
        #{
            strategy => one_for_one,
            intensity => 2,
            period => 10
        },
    Children =
        [
            #{
                id => license_checker,
                start => {emqx_license_checker, start_link, [Reader]},
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
        ],
    {ok, {Strategy, Children}}.
