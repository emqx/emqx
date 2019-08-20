%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_cluster_mon).

-behaviour(gen_server).

-include("logger.hrl").

-logger_header("[CLUSTER_MON]").

-export([start_link/0]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    ekka:monitor(partition),
    {ok, #{}}.

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({partition, {occurred, Node}}, State) ->
    alarm_handler:set_alarm({partitioned, Node}),
    {noreply, State};

handle_info({partition, {healed, Nodes}}, State) ->
    alarm_handler:clear_alarm(partitioned),
    case ekka_mnesia:running_nodes() -- [node() | Nodes] of
        [] -> ignore;
        Nodes2 ->
            emqx_rpc:multicall(Nodes2, alarm_handler, clear_alarm, [partitioned])
    end,
    {noreply, State};

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ekka:unmonitor(partition),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
