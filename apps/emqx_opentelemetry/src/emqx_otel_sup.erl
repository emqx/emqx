%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_otel_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).
-export([worker_spec/2]).

worker_spec(Mod, Opts) ->
    #{
        id => Mod,
        start => {Mod, start_link, [Opts]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [Mod]
    }.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 512
    },
    Children =
        case emqx_conf:get([opentelemetry]) of
            #{metrics := #{enable := false}} ->
                [];
            #{metrics := #{enable := true}} = Conf ->
                [
                    worker_spec(emqx_otel_metrics, Conf),
                    worker_spec(emqx_otel_cpu_sup, Conf)
                ]
        end,
    {ok, {SupFlags, Children}}.
