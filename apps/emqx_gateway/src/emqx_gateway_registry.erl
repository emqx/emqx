%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_gateway_registry).

-include("include/emqx_gateway.hrl").

-logger_header("[PGW-Registry]").

-behavior(gen_server).

%% APIs for Impl.
-export([ load/3
        , unload/1
        ]).

-export([ list/0
        , lookup/1
        ]).

%% APIs
-export([start_link/0]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {
          loaded = #{} :: #{ gateway_id() => descriptor() }
         }).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% Mgmt
%%--------------------------------------------------------------------

-type registry_options() :: list().

-type gateway_options() :: list().

-type descriptor() :: #{ cbkmod := atom()
                       , rgopts := registry_options()
                       , gwopts := gateway_options()
                       , state  => any()
                       }.

-spec load(GwId :: atom(), registry_options(), list()) -> ok | {error, any()}.

load(GwId, RgOpts, GwOpts) ->
    CbMod = proplists:get_value(cbkmod, RgOpts, GwId),
    Dscrptr = #{ cbkmod => CbMod
               , rgopts => RgOpts
               , gwopts => GwOpts
               },
    call({load, GwId, Dscrptr}).

-spec unload(GwId :: atom()) -> ok | {error, any()}.

unload(GwId) ->
    call({unload, GwId}).

%% @doc Return all registered protocol gateway implementation
-spec list() -> [atom()].
list() ->
    call(all).

-spec lookup(atom()) -> emqx_gateway_impl:state().
lookup(GwId) ->
    call({lookup, GwId}).

call(Req) ->
    gen_server:call(?MODULE, Req, 5000).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

%% TODO: Metrics ???

init([]) ->
    process_flag(trap_exit, true),
    {ok, #state{loaded = #{}}}.

handle_call({load, GwId, Dscrptr}, _From, State = #state{loaded = Gateways}) ->
    case maps:get(GwId, Gateways, notfound) of
        notfound ->
            try
                GwOpts = maps:get(gwopts, Dscrptr),
                CbMod  = maps:get(cbkmod, Dscrptr),
                {ok, GwState} = CbMod:init(GwOpts),
                NDscrptr = maps:put(state, GwState, Dscrptr),
                NGateways = maps:put(GwId, NDscrptr, Gateways),
                {reply, ok, State#state{loaded = NGateways}}
            catch
                Class : Reason : Stk ->
                    logger:error("Load ~s crashed {~p, ~p}; stacktrace: ~0p",
                                  [GwId, Class, Reason, Stk]),
                    {reply, {error, {Class, Reason}}, State}
            end;
        _ ->
            {reply, {error, already_existed}, State}
    end;

handle_call({unload, GwId}, _From, State = #state{loaded = Gateways}) ->
    case maps:get(GwId, Gateways, undefined) of
        undefined ->
            {reply, ok, State};
        _ ->
            emqx_gateway_sup:stop_all_suptree(GwId),
            {reply, ok, State#state{loaded = maps:remove(GwId, Gateways)}}
    end;

handle_call(all, _From, State = #state{loaded = Gateways}) ->
    Reply = maps:values(Gateways),
    {reply, Reply, State};

handle_call({lookup, GwId}, _From, State = #state{loaded = Gateways}) ->
    Reply = maps:get(GwId, Gateways, undefined),
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
