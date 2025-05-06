%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This supervisor manages workers which should never need a restart
%% due to config changes or when joining a cluster.
-module(emqx_machine_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Terminator = child_worker(emqx_machine_terminator, [], transient),
    BootApps = child_worker(emqx_machine_boot, post_boot, [], temporary),
    GlobalGC = child_worker(emqx_global_gc, [], permanent),
    ReplicantHealthProbe = child_worker(emqx_machine_replicant_health_probe, [], transient),
    Children = [Terminator, ReplicantHealthProbe, BootApps, GlobalGC],
    SupFlags = #{
        strategy => one_for_one,
        intensity => 100,
        period => 10
    },
    {ok, {SupFlags, Children}}.

child_worker(M, Args, Restart) ->
    child_worker(M, start_link, Args, Restart).

child_worker(M, Func, Args, Restart) ->
    #{
        id => M,
        start => {M, Func, Args},
        restart => Restart,
        shutdown => 5000,
        type => worker,
        modules => [M]
    }.
