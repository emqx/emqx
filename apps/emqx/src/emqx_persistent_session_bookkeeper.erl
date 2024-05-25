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
    get_subscription_count/0,
    get_disconnected_session_count/0,
    get_disconnected_session_count/1,

    inc_disconnected_sessions/1,
    dec_disconnected_sessions/1,

    inc_subs/0,
    dec_subs/0
]).

%% `gen_server' API
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(TAB, emqx_persistent_session_ds_stats).
-define(STATS_SHARD, emqx_persistent_session_ds_stats_shard).

%% Gauge record:
-record(?TAB, {key, gauge}).

%% Stats keys:
-record(disconnected_sessions, {node :: node()}).
-define(subscriptions, subscriptions).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link() -> gen_server:start_ret().
start_link() ->
    ok = mria:create_table(?TAB, [
        {type, set},
        {rlog_shard, ?STATS_SHARD},
        {storage, disc_copies},
        {attributes, record_info(fields, ?TAB)}
    ]),
    _ = mria:wait_for_tables([?TAB]),
    gen_server:start_link({local, ?MODULE}, ?MODULE, _InitOpts = #{}, _Opts = []).

%% @doc Gets a cached view of the cluster-global count of persistent subscriptions.
-spec get_subscription_count() -> non_neg_integer().
get_subscription_count() ->
    case is_enabled() of
        true ->
            case mnesia:dirty_read(?TAB, ?subscriptions) of
                [#?TAB{gauge = Gauge}] ->
                    Gauge;
                [] ->
                    0
            end;
        false ->
            0
    end.

%% @doc Gets a cached view of the cluster-global count of disconnected persistent sessions.
-spec get_disconnected_session_count() -> non_neg_integer().
get_disconnected_session_count() ->
    case is_enabled() of
        true ->
            MS = {#?TAB{key = #disconnected_sessions{_ = '_'}, gauge = '$1'}, [], ['$1']},
            lists:sum(mnesia:dirty_select(?TAB, [MS]));
        false ->
            0
    end.

-spec get_disconnected_session_count(node()) -> non_neg_integer().
get_disconnected_session_count(Node) ->
    case is_enabled() of
        true ->
            case mnesia:dirty_read(?TAB, #disconnected_sessions{node = Node}) of
                [#?TAB{gauge = Gauge}] ->
                    Gauge;
                [] ->
                    0
            end;
        false ->
            0
    end.

-spec inc_disconnected_sessions(node()) -> ok.
inc_disconnected_sessions(Node) ->
    mria:dirty_update_counter(?TAB, #disconnected_sessions{node = Node}, 1),
    ok.

-spec dec_disconnected_sessions(node()) -> ok.
dec_disconnected_sessions(Node) ->
    mria:dirty_update_counter(?TAB, #disconnected_sessions{node = Node}, -1),
    ok.

-spec inc_subs() -> ok.
inc_subs() ->
    mria:dirty_update_counter(?TAB, ?subscriptions, 1),
    ok.

-spec dec_subs() -> ok.
dec_subs() ->
    mria:dirty_update_counter(?TAB, ?subscriptions, -1),
    ok.

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(_Opts) ->
    case emqx_persistent_message:is_persistence_enabled() of
        true ->
            State = #{
                subs_count => 0,
                disconnected_session_count => 0
            },
            {ok, State};
        false ->
            ignore
    end.

handle_call(_Call, _From, State) ->
    {reply, {error, bad_call}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

is_enabled() ->
    emqx_persistent_message:is_persistence_enabled() andalso whereis(?MODULE) =/= undefined.
