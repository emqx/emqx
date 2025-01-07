%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc The Registry Centre of Gateway
-module(emqx_gateway_registry).

-include("emqx_gateway.hrl").

-behaviour(gen_server).

%% APIs
-export([
    reg/2,
    unreg/1,
    list/0,
    lookup/1
]).

%% APIs
-export([start_link/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export_type([descriptor/0]).

-record(state, {
    reged = #{} :: #{gateway_name() => descriptor()}
}).

-type registry_options() :: [registry_option()].

-type registry_option() :: {cbkmod, atom()}.

-type descriptor() :: #{
    cbkmod := atom(),
    rgopts := registry_options()
}.

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% Mgmt
%%--------------------------------------------------------------------

-spec reg(gateway_name(), registry_options()) ->
    ok
    | {error, any()}.
reg(Name, RgOpts) ->
    CbMod = proplists:get_value(cbkmod, RgOpts, Name),
    Dscrptr = #{
        cbkmod => CbMod,
        rgopts => RgOpts
    },
    call({reg, Name, Dscrptr}).

-spec unreg(gateway_name()) -> ok | {error, any()}.
unreg(Name) ->
    %% TODO: Checking ALL INSTANCE HAS STOPPED
    call({unreg, Name}).

%% TODO:
%unreg(Name, Force) ->
%    call({unreg, Name, Froce}).

%% @doc Return all registered protocol gateway implementation
-spec list() -> [{gateway_name(), descriptor()}].
list() ->
    call(all).

-spec lookup(gateway_name()) -> undefined | descriptor().
lookup(Name) ->
    call({lookup, Name}).

call(Req) ->
    gen_server:call(?MODULE, Req, 5000).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    {ok, #state{reged = #{}}}.

handle_call({reg, Name, Dscrptr}, _From, State = #state{reged = Gateways}) ->
    case maps:get(Name, Gateways, notfound) of
        notfound ->
            NGateways = maps:put(Name, Dscrptr, Gateways),
            {reply, ok, State#state{reged = NGateways}};
        _ ->
            {reply, {error, already_existed}, State}
    end;
handle_call({unreg, Name}, _From, State = #state{reged = Gateways}) ->
    case maps:get(Name, Gateways, undefined) of
        undefined ->
            {reply, ok, State};
        _ ->
            _ = emqx_gateway_sup:unload_gateway(Name),
            {reply, ok, State#state{reged = maps:remove(Name, Gateways)}}
    end;
handle_call(all, _From, State = #state{reged = Gateways}) ->
    {reply, maps:to_list(Gateways), State};
handle_call({lookup, Name}, _From, State = #state{reged = Gateways}) ->
    Reply = maps:get(Name, Gateways, undefined),
    {reply, Reply, State};
handle_call(Req, _From, State) ->
    logger:error("Unexpected call: ~0p", [Req]),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
