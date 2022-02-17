%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_dashboard_monitor).
-behaviour(gen_server).

-export([start_link/0]).

-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-export([ mnesia/1]).

-define(TAB, ?MODULE).

-record(state, {}).
-define(INIT_DATA,
    #{
        sent            => 0,
        received        => 0,
        sent_bytes      => 0,
        received_bytes  => 0,
        dropped         => 0,
        subscriptions   => 0,
        routes          => 0,
        connection      => 0
    }).

-record(monitor_data)

mnesia(boot) ->
    ok = mria:create_table(?TAB, [
        {type, set},
        {local_content, true},
        {storage, disc_copies},
        {record_name, monitor_data},
        {attributes, record_info(fields, monitor_data)}]).

start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, #state{}}.

handle_call(_Request, _From, State = #state{}) ->
    {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
    {noreply, State}.

handle_info(_Info, State = #state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #state{}) ->
    ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

next_interval() ->
    ExpireInterval = emqx_conf:get([dashboard, monitor, interval], ?EXPIRE_INTERVAL),
    ok.

monit() ->
    ok.
