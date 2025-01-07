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
-module(emqx_broker_mon).

-feature(maybe_expr, enable).

-behaviour(gen_server).

%% API
-export([
    start_link/0,

    get_mnesia_tm_mailbox_size/0,
    get_broker_pool_max_mailbox_size/0
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

-define(PT_KEY, {?MODULE, atomics}).
-define(mnesia_tm_mailbox, mnesia_tm_mailbox).
-define(broker_pool_max_mailbox, broker_pool_max_mailbox).
-define(GAUGES, #{
    ?mnesia_tm_mailbox => 1,
    ?broker_pool_max_mailbox => 2
}).

-ifdef(TEST).
-define(UPDATE_INTERVAL_MS, persistent_term:get({?MODULE, update_interval}, 10_000)).
-else.
-define(UPDATE_INTERVAL_MS, 10_000).
-endif.

-define(update, update).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec get_mnesia_tm_mailbox_size() -> non_neg_integer().
get_mnesia_tm_mailbox_size() ->
    case persistent_term:get(?PT_KEY, undefined) of
        undefined ->
            0;
        Atomics ->
            Idx = maps:get(?mnesia_tm_mailbox, ?GAUGES),
            atomics:get(Atomics, Idx)
    end.

-spec get_broker_pool_max_mailbox_size() -> non_neg_integer().
get_broker_pool_max_mailbox_size() ->
    case persistent_term:get(?PT_KEY, undefined) of
        undefined ->
            0;
        Atomics ->
            Idx = maps:get(?broker_pool_max_mailbox, ?GAUGES),
            atomics:get(Atomics, Idx)
    end.

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(_) ->
    Atomics = ensure_atomics(),
    State = #{atomics => Atomics},
    start_update_timer(),
    {ok, State}.

handle_call(_Call, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info({timeout, _TRef, ?update}, State) ->
    ok = handle_update(State),
    start_update_timer(),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

ensure_atomics() ->
    case persistent_term:get(?PT_KEY, undefined) of
        undefined ->
            Atomics = atomics:new(map_size(?GAUGES), [{signed, false}]),
            persistent_term:put(?PT_KEY, Atomics),
            Atomics;
        Atomics ->
            Atomics
    end.

handle_update(State) ->
    MnesiaTMMailbox = update_mnesia_tm_mailbox_size(State),
    BrokerPoolMaxMailbox = update_broker_pool_max_mailbox_size(State),
    toggle_alarm(?mnesia_tm_mailbox, MnesiaTMMailbox),
    toggle_alarm(?broker_pool_max_mailbox, BrokerPoolMaxMailbox),
    ok.

toggle_alarm(?mnesia_tm_mailbox, MailboxSize) ->
    Name = <<"mnesia_transaction_manager_overload">>,
    Details = #{mailbox_size => MailboxSize},
    Message = [<<"mnesia overloaded; mailbox size: ">>, integer_to_binary(MailboxSize)],
    case MailboxSize > mnesia_tm_threshold() of
        true ->
            _ = emqx_alarm:activate(Name, Details, Message),
            ok;
        false ->
            ensure_alarm_deactivated(Name, Details, Message),
            ok
    end;
toggle_alarm(?broker_pool_max_mailbox, MailboxSize) ->
    Name = <<"broker_pool_overload">>,
    Details = #{mailbox_size => MailboxSize},
    Message = [<<"broker pool overloaded; mailbox size: ">>, integer_to_binary(MailboxSize)],
    case MailboxSize > broker_pool_threshold() of
        true ->
            _ = emqx_alarm:activate(Name, Details, Message),
            ok;
        false ->
            ensure_alarm_deactivated(Name, Details, Message),
            ok
    end.

ensure_alarm_deactivated(Name, Details, Message) ->
    try
        _ = emqx_alarm:ensure_deactivated(Name, Details, Message),
        ok
    catch
        _:_ ->
            %% When a node is leaving/joining a cluster, it may happen that the table is
            %% deleted, and the dirty read in `emqx_alarm:ensure_deactivated' aborts with
            %% `{no_exists, _}'.
            ok
    end.

update_mnesia_tm_mailbox_size(State) ->
    #{atomics := Atomics} = State,
    #{?mnesia_tm_mailbox := Idx} = ?GAUGES,
    MailboxSize = read_mnesia_tm_mailbox_size(),
    atomics:put(Atomics, Idx, MailboxSize),
    MailboxSize.

update_broker_pool_max_mailbox_size(State) ->
    #{atomics := Atomics} = State,
    #{?broker_pool_max_mailbox := Idx} = ?GAUGES,
    MailboxSize = read_broker_pool_max_mailbox_size(),
    atomics:put(Atomics, Idx, MailboxSize),
    MailboxSize.

read_mnesia_tm_mailbox_size() ->
    maybe
        Pid = whereis(mnesia_tm),
        true ?= is_pid(Pid),
        {message_queue_len, Len} ?= process_info(Pid, message_queue_len),
        Len
    else
        _ -> 0
    end.

read_broker_pool_max_mailbox_size() ->
    Workers = emqx_broker_sup:get_broker_pool_workers(),
    MailboxSizes =
        lists:flatmap(
            fun(Worker) ->
                case process_info(Worker, message_queue_len) of
                    {message_queue_len, N} ->
                        [N];
                    undefined ->
                        []
                end
            end,
            Workers
        ),
    lists:max([0 | MailboxSizes]).

start_update_timer() ->
    _ = emqx_utils:start_timer(?UPDATE_INTERVAL_MS, ?update),
    ok.

mnesia_tm_threshold() ->
    emqx_config:get([sysmon, mnesia_tm_mailbox_size_alarm_threshold]).

broker_pool_threshold() ->
    emqx_config:get([sysmon, broker_pool_mailbox_size_alarm_threshold]).
