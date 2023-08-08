%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-export([start_otel/1]).
-export([stop_otel/0]).

-define(CHILD(Mod, Opts), #{
    id => Mod,
    start => {Mod, start_link, [Opts]},
    restart => permanent,
    shutdown => 5000,
    type => worker,
    modules => [Mod]
}).

-define(WORKER, emqx_otel).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_otel(map()) -> ok.
start_otel(Conf) ->
    assert_started(supervisor:start_child(?MODULE, ?CHILD(?WORKER, Conf))).

-spec stop_otel() -> ok | {error, term()}.
stop_otel() ->
    case supervisor:terminate_child(?MODULE, ?WORKER) of
        ok -> supervisor:delete_child(?MODULE, ?WORKER);
        {error, not_found} -> ok;
        Error -> Error
    end.

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 512
    },
    Children =
        case emqx_conf:get([opentelemetry]) of
            #{enable := false} -> [];
            #{enable := true} = Conf -> [?CHILD(?WORKER, Conf)]
        end,
    {ok, {SupFlags, Children}}.

assert_started({ok, _Pid}) -> ok;
assert_started({ok, _Pid, _Info}) -> ok;
assert_started({error, {already_started, _Pid}}) -> ok;
assert_started({error, Reason}) -> {error, Reason}.
