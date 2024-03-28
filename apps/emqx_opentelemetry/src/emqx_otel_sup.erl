%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
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
