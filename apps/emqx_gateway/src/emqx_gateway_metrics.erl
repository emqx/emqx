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

-module(emqx_gateway_metrics).

-behaviour(gen_server).

-include("include/emqx_gateway.hrl").

-logger_header("[PGW-Metrics]").

%% APIs
-export([start_link/1]).

-export([ inc/2
        , inc/3
        , dec/2
        , dec/3
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-export([tabname/1]).

-record(state, {}).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link(Type) ->
    gen_server:start_link(?MODULE, [Type], []).

-spec inc(gateway_type(), atom()) -> ok.
inc(Type, Name) ->
    inc(Type, Name, 1).

-spec inc(gateway_type(), atom(), integer()) -> ok.
inc(Type, Name, Oct) ->
    ets:update_counter(tabname(Type), Name, {2, Oct}, {Name, 0}),
    ok.

-spec dec(gateway_type(), atom()) -> ok.
dec(Type, Name) ->
    inc(Type, Name, -1).

-spec dec(gateway_type(), atom(), non_neg_integer()) -> ok.
dec(Type, Name, Oct) ->
    inc(Type, Name, -Oct).

tabname(Type) ->
    list_to_atom(lists:concat([emqx_gateway_, Type, '_metrics'])).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Type]) ->
    TabOpts = [public, {write_concurrency, true}],
    ok = emqx_tables:new(tabname(Type), [set|TabOpts]),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------
