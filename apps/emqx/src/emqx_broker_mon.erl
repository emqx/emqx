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
    get_broker_pool_max_mailbox_size/0,
    get_mailbox_size_top_n/0
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
-define(MAILBOX_LEN_TAB, emqx_proc_mailbox_len).

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

-spec get_mailbox_size_top_n() -> [{binary(), non_neg_integer()}].
get_mailbox_size_top_n() ->
    ets:tab2list(?MAILBOX_LEN_TAB).

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(_) ->
    ets:new(?MAILBOX_LEN_TAB, [named_table, set, public]),
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
    case overall_mailbox_mon_enabled() of
        true ->
            ProcsInfo = non_zero_mailbox_procs(overall_mailbox_mon_max_procs()),
            update_top_n_mailbox_procs(ProcsInfo),
            toggle_overall_mailbox_alarm(ProcsInfo);
        false ->
            ok
    end,
    MnesiaTMMailbox = update_mnesia_tm_mailbox_size(State),
    BrokerPoolMaxMailbox = update_broker_pool_max_mailbox_size(State),
    toggle_alarm(?mnesia_tm_mailbox, MnesiaTMMailbox),
    toggle_alarm(?broker_pool_max_mailbox, BrokerPoolMaxMailbox),
    ok.

toggle_overall_mailbox_alarm(ProcsInfo) ->
    Name = <<"process_overload">>,
    Message = make_top_n_mailbox_alarm_msg(ProcsInfo),
    case overall_mailbox_alarm_enabled() of
        true ->
            case make_top_n_mailbox_alarm_detail(ProcsInfo) of
                [] ->
                    ensure_alarm_deactivated(Name, #{}, Message);
                AlarmMsg ->
                    _ = emqx_alarm:activate(Name, AlarmMsg, Message)
            end;
        false ->
            ok
    end.

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

update_top_n_mailbox_procs(ProcsInfo) ->
    ets:delete_all_objects(?MAILBOX_LEN_TAB),
    lists:foreach(
        fun({Pid, QLen, ProcName}) ->
            ets:insert(?MAILBOX_LEN_TAB, {Pid, ProcName, QLen, make_proc_meta_data(Pid)})
        end,
        ProcsInfo
    ).

make_proc_meta_data(Pid) ->
    case erlang:process_info(Pid, dictionary) of
        undefined ->
            #{};
        {dictionary, Dict} ->
            LoggerProcMeta = proplists:get_value('$logger_metadata$', Dict, #{}),
            emqx_utils_maps:jsonable_map(
                LoggerProcMeta,
                fun(K, V) ->
                    {emqx_utils_conv:bin(K), emqx_utils_conv:bin(V)}
                end
            )
    end.

non_zero_mailbox_procs(TopN) ->
    [
        {Pid, QLen, proc_name(Pid)}
     || {Pid, QLen, _} <- recon:proc_count(message_queue_len, TopN), QLen > 0
    ].

make_top_n_mailbox_alarm_msg([]) ->
    <<"some processes are overloaded">>;
make_top_n_mailbox_alarm_msg(ProcsInfo) ->
    [
        <<"some processes are overloaded, overloaded procs:">>,
        [
            [<<" {">>, ProcName, <<": ">>, integer_to_binary(QLen), <<"}">>]
         || {_Pid, QLen, ProcName} <- ProcsInfo
        ]
    ].

make_top_n_mailbox_alarm_detail([]) ->
    [];
make_top_n_mailbox_alarm_detail([{_Pid, QLen, _} | _] = ProcsInfo) ->
    AlarmTopN = overall_mailbox_alarm_top_n(),
    case QLen >= overall_mailbox_alarm_threshold() of
        true ->
            ProcsToBeAlarm = [Pid || {Pid, _, _} <- lists:sublist(ProcsInfo, 1, AlarmTopN)],
            format_procs_info(ProcsToBeAlarm, verbose);
        false ->
            []
    end.

format_procs_info(PidL, Type) ->
    lists:filtermap(
        fun(Pid) ->
            case format_proc_info(get_pid(Pid), Type) of
                undefined -> false;
                Info -> {true, Info}
            end
        end,
        PidL
    ).

basic_proc_info(Pid, Name, Init, Dict, QLen, Verbose) ->
    Info0 = #{
        pid => list_to_binary(pid_to_list(Pid)),
        registered_name => reg_name(Name),
        initial_call => init_call(Init, Dict),
        label => get_label(Pid)
    },
    maybe_peek_messages(Pid, Info0, QLen, Dict, Verbose).

maybe_peek_messages(Pid, Info0, QLen, Dict, verbose) when QLen > 0, QLen < 1000 ->
    Info = Info0#{message_queue_len => QLen, proc_dict_len => length(Dict)},
    case erlang:process_info(Pid, messages) of
        {messages, Messages} ->
            Info#{
                messages_first_n => format_messages(
                    Messages, overall_mailbox_alarm_peek_first_n_msgs()
                )
            };
        _ ->
            Info
    end;
maybe_peek_messages(_Pid, Info0, QLen, Dict, verbose) ->
    Info0#{message_queue_len => QLen, proc_dict_len => length(Dict)};
maybe_peek_messages(_Pid, Info, _QLen, _Dict, _) ->
    Info.

format_proc_info(undefined, _) ->
    undefined;
format_proc_info(Pid, verbose) when is_pid(Pid) ->
    Attrs = [registered_name, initial_call, dictionary, message_queue_len, current_stacktrace],
    case erlang:process_info(Pid, Attrs) of
        [
            {registered_name, Name},
            {initial_call, Init},
            {dictionary, Dict},
            {message_queue_len, QLen},
            {current_stacktrace, StackTrace}
        ] ->
            Info = basic_proc_info(Pid, Name, Init, Dict, QLen, verbose),
            Info#{
                current_stacktrace => format_stacktrace(StackTrace),
                ancestors => format_ancestors(Dict)
            };
        undefined ->
            undefined
    end;
format_proc_info(Pid, normal) when is_pid(Pid) ->
    Attrs = [registered_name, initial_call, dictionary, message_queue_len],
    case erlang:process_info(Pid, Attrs) of
        [
            {registered_name, Name},
            {initial_call, Init},
            {dictionary, Dict},
            {message_queue_len, QLen}
        ] ->
            basic_proc_info(Pid, Name, Init, Dict, QLen, normal);
        undefined ->
            undefined
    end.

-spec proc_name(pid()) -> binary().
proc_name(Pid) ->
    case erlang:process_info(Pid, [registered_name, initial_call, dictionary]) of
        [{registered_name, Name} | _] when is_atom(Name), Name =/= undefined ->
            atom_to_binary(Name);
        [{registered_name, _Name}, {initial_call, Init}, {dictionary, Dict}] ->
            case get_label(Pid) of
                undefined -> init_call(Init, Dict);
                Label -> Label
            end;
        _ ->
            <<"undefined">>
    end.

reg_name(Name) when is_atom(Name) -> Name;
reg_name(_) -> undefined.

init_call(Init, Dict) ->
    case proplists:get_value('$initial_call', Dict) of
        undefined -> format_mfa(Init);
        Init1 -> format_mfa(Init1)
    end.

get_label(Pid) ->
    case
        erlang:function_exported(proc_lib, get_label, 1) andalso
            proc_lib:get_label(Pid)
    of
        false -> undefined;
        undefined -> undefined;
        Label -> iolist_to_binary(io_lib:format("~p", [Label]))
    end.

format_messages([], _) -> [];
format_messages(_Msgs, 0) -> [];
format_messages([Term | Msgs], N) -> [format_term(Term) | format_messages(Msgs, N - 1)].

format_mfa({M, F, A}) ->
    iolist_to_binary(io_lib:format("~s:~s/~p", [atom_to_list(M), atom_to_list(F), A])).

format_stacktrace(StackTrace) ->
    lists:map(fun format_stack/1, StackTrace).

format_stack({M, F, A, Location}) ->
    #{
        mfa => format_mfa({M, F, A}),
        location => format_location(Location)
    }.

format_location(Location) ->
    File = proplists:get_value(file, Location, unknown_file),
    Line = proplists:get_value(line, Location, unknown_line),
    iolist_to_binary(io_lib:format("~s:~p", [File, Line])).

format_ancestors(Dict) ->
    case proplists:get_value('$ancestors', Dict) of
        undefined -> [];
        Ancestors -> format_procs_info(Ancestors, normal)
    end.

get_pid(Pid) when is_pid(Pid) -> Pid;
get_pid(NameOrPid) when is_atom(NameOrPid) ->
    case erlang:whereis(NameOrPid) of
        undefined -> undefined;
        Pid -> Pid
    end;
get_pid(_) ->
    undefined.

format_term(Term) when is_atom(Term); is_integer(Term); is_float(Term) ->
    Term;
format_term(Term) ->
    iolist_to_binary(io_lib:fwrite("~p", [Term], [{chars_limit, 512}])).

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

overall_mailbox_mon_enabled() ->
    emqx_config:get([sysmon, overall_mailbox_size_monitor, enable]).

overall_mailbox_mon_max_procs() ->
    emqx_config:get([sysmon, overall_mailbox_size_monitor, max_procs]).

overall_mailbox_alarm_enabled() ->
    overall_mailbox_alarm_threshold() > 0.

overall_mailbox_alarm_threshold() ->
    emqx_config:get([sysmon, overall_mailbox_size_monitor, alarm_threshold]).

overall_mailbox_alarm_top_n() ->
    emqx_config:get([sysmon, overall_mailbox_size_monitor, alarm_top_n_procs]).

overall_mailbox_alarm_peek_first_n_msgs() ->
    emqx_config:get([sysmon, overall_mailbox_size_monitor, alarm_try_peek_first_n_msgs]).
