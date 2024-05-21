%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_persistent_session_bookkeeper).

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    get_subscription_count/0
]).

%% `gen_server' API
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%% call/cast/info events
-record(tally_subs, {}).
-record(get_subscription_count, {}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, _InitOpts = #{}, _Opts = []).

%% @doc Gets a cached view of the cluster-global count of persistent subscriptions.
-spec get_subscription_count() -> non_neg_integer().
get_subscription_count() ->
    case emqx_persistent_message:is_persistence_enabled() of
        true ->
            gen_server:call(?MODULE, #get_subscription_count{}, infinity);
        false ->
            0
    end.

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(_Opts) ->
    case emqx_persistent_message:is_persistence_enabled() of
        true ->
            State = #{subs_count => 0},
            {ok, State, {continue, #tally_subs{}}};
        false ->
            ignore
    end.

handle_continue(#tally_subs{}, State0) ->
    State = tally_persistent_subscriptions(State0),
    ensure_subs_tally_timer(),
    {noreply, State}.

handle_call(#get_subscription_count{}, _From, State) ->
    #{subs_count := N} = State,
    {reply, N, State};
handle_call(_Call, _From, State) ->
    {reply, {error, bad_call}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(#tally_subs{}, State0) ->
    State = tally_persistent_subscriptions(State0),
    ensure_subs_tally_timer(),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

tally_persistent_subscriptions(State0) ->
    N = emqx_persistent_session_ds_state:total_subscription_count(),
    State0#{subs_count := N}.

ensure_subs_tally_timer() ->
    Timeout = emqx_config:get([durable_sessions, subscription_count_refresh_interval]),
    _ = erlang:send_after(Timeout, self(), #tally_subs{}),
    ok.
