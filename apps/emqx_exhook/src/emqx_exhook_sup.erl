%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exhook_sup).

-behaviour(supervisor).

-export([ start_link/0
        , init/1
        ]).

-export([ start_service_channel/3
        , stop_service_channel/1
        ]).

%%--------------------------------------------------------------------
%%  Supervisor APIs & Callbacks
%%--------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_one, 10, 100}, []}}.

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec start_service_channel(
        atom(),
        [grpcbox_channel:endpoint()],
        grpcbox_channel:options()) -> {ok, pid()} | {error, term()}.
start_service_channel(Name, Endpoints, Options) ->
    Spec = #{id => Name,
             start => {grpcbox_channel, start_link, [Name, Endpoints, Options]},
             type => worker},
    case supervisor:start_child(?MODULE, Spec) of
        {ok, Pid} ->
            wait_ready(Name, {ok, Pid});
        {error, {already_started, Pid}} ->
            wait_ready(Name, {ok, Pid});
        {error, Reason} ->
            {error, Reason}
    end.

-spec stop_service_channel(atom()) -> ok.
stop_service_channel(Name) ->
    ok = supervisor:terminate_child(?MODULE, Name),
    ok = supervisor:delete_child(?MODULE, Name).

%% @private
wait_ready(Name, Ret) ->
    wait_ready(1500, Name, Ret).

wait_ready(0, _, _) ->
    {error, waiting_ready_timeout};
wait_ready(Num, Name, Ret) ->
    case grpcbox_channel:is_ready(Name) of
        true -> Ret;
        _ ->
            timer:sleep(10),
            wait_ready(Num-1, Name, Ret)
    end.

