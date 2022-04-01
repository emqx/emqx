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

-module(emqx_gateway_metrics).

-behaviour(gen_server).

-include_lib("emqx_gateway/include/emqx_gateway.hrl").

%% APIs
-export([start_link/1]).

-export([
    inc/2,
    inc/3,
    dec/2,
    dec/3
]).

-export([lookup/1]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export([tabname/1]).

-record(state, {}).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link(GwName) ->
    gen_server:start_link(?MODULE, [GwName], []).

-spec inc(gateway_name(), atom()) -> ok.
inc(GwName, Name) ->
    inc(GwName, Name, 1).

-spec inc(gateway_name(), atom(), integer()) -> ok.
inc(GwName, Name, Oct) ->
    ets:update_counter(tabname(GwName), Name, {2, Oct}, {Name, 0}),
    ok.

-spec dec(gateway_name(), atom()) -> ok.
dec(GwName, Name) ->
    inc(GwName, Name, -1).

-spec dec(gateway_name(), atom(), non_neg_integer()) -> ok.
dec(GwName, Name, Oct) ->
    inc(GwName, Name, -Oct).

-spec lookup(gateway_name()) ->
    undefined
    | [{Name :: atom(), integer()}].
lookup(GwName) ->
    Tab = emqx_gateway_metrics:tabname(GwName),
    case ets:info(Tab) of
        undefined -> undefined;
        _ -> lists:sort(ets:tab2list(Tab))
    end.

tabname(GwName) ->
    list_to_atom(lists:concat([emqx_gateway_, GwName, '_metrics'])).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([GwName]) ->
    TabOpts = [public, {write_concurrency, true}],
    ok = emqx_tables:new(tabname(GwName), [set | TabOpts]),
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
