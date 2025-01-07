%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    get_subscription_count/0,
    get_disconnected_session_count/0
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
-record(tally_disconnected_sessions, {}).

%%------------------------------------------------------------------------------
%% Stat records & table
%%------------------------------------------------------------------------------

-record(stat_field, {name, value}).

-define(tab, ?MODULE).
-define(subs_count, subs_count).
-define(disconnected_session_count, disconnected_session_count).
-define(subs_count(VALUE), #stat_field{name = ?subs_count, value = VALUE}).
-define(disconnected_session_count(VALUE), #stat_field{
    name = ?disconnected_session_count, value = VALUE
}).

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
            try ets:lookup(?tab, ?subs_count) of
                [?subs_count(N)] -> N;
                [] -> 0
            catch
                error:badarg -> 0
            end;
        false ->
            0
    end.

%% @doc Gets a cached view of the cluster-global count of disconnected persistent sessions.
-spec get_disconnected_session_count() -> non_neg_integer().
get_disconnected_session_count() ->
    case emqx_persistent_message:is_persistence_enabled() of
        true ->
            try ets:lookup(?tab, ?disconnected_session_count) of
                [?disconnected_session_count(N)] -> N;
                [] -> 0
            catch
                error:badarg -> 0
            end;
        false ->
            0
    end.

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(_Opts) ->
    case emqx_persistent_message:is_persistence_enabled() of
        true ->
            State = #{
                tab => ets:new(?tab, [named_table, set, protected, {keypos, #stat_field.name}])
            },
            {ok, State, {continue, #tally_subs{}}};
        false ->
            ignore
    end.

handle_continue(#tally_subs{}, State0) ->
    State = tally_persistent_subscriptions(State0),
    ensure_subs_tally_timer(),
    {noreply, State, {continue, #tally_disconnected_sessions{}}};
handle_continue(#tally_disconnected_sessions{}, State0) ->
    State = tally_disconnected_persistent_sessions(State0),
    ensure_disconnected_sessions_tally_timer(),
    {noreply, State}.

handle_call(_Call, _From, State) ->
    {reply, {error, bad_call}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(#tally_subs{}, State0) ->
    State = tally_persistent_subscriptions(State0),
    ensure_subs_tally_timer(),
    {noreply, State};
handle_info(#tally_disconnected_sessions{}, State0) ->
    State = tally_disconnected_persistent_sessions(State0),
    ensure_disconnected_sessions_tally_timer(),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

tally_persistent_subscriptions(#{tab := Tab} = State) ->
    N = emqx_persistent_session_ds_state:total_subscription_count(),
    _ = ets:insert(Tab, ?subs_count(N)),
    State.

tally_disconnected_persistent_sessions(#{tab := Tab} = State) ->
    N = do_tally_disconnected_persistent_sessions(),
    _ = ets:insert(Tab, ?disconnected_session_count(N)),
    State.

ensure_subs_tally_timer() ->
    Timeout = emqx_config:get([durable_sessions, subscription_count_refresh_interval]),
    _ = erlang:send_after(Timeout, self(), #tally_subs{}),
    ok.

ensure_disconnected_sessions_tally_timer() ->
    Timeout = emqx_config:get([durable_sessions, disconnected_session_count_refresh_interval]),
    _ = erlang:send_after(Timeout, self(), #tally_disconnected_sessions{}),
    ok.

do_tally_disconnected_persistent_sessions() ->
    Iter = emqx_persistent_session_ds_state:make_session_iterator(),
    do_tally_disconnected_persistent_sessions(Iter, 0).

do_tally_disconnected_persistent_sessions('$end_of_table', N) ->
    N;
do_tally_disconnected_persistent_sessions(Iter0, N) ->
    case emqx_persistent_session_ds_state:session_iterator_next(Iter0, 1) of
        {[], _} ->
            N;
        {[{Id, _Meta}], Iter} ->
            case is_live_session(Id) of
                true ->
                    do_tally_disconnected_persistent_sessions(Iter, N);
                false ->
                    do_tally_disconnected_persistent_sessions(Iter, N + 1)
            end
    end.

is_live_session(Id) ->
    [] =/= emqx_cm_registry:lookup_channels(Id).
