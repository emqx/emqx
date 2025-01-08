%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_sys_mon).

-behaviour(gen_server).

-include("types.hrl").
-include("logger.hrl").

-export([start_link/0]).

%% compress unused warning
-export([procinfo/1]).

%% gen_server callbacks
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).
-export([add_handler/0, remove_handler/0, post_config_update/5]).
-export([update/1]).

-define(SYSMON, ?MODULE).
-define(SYSMON_CONF_ROOT, [sysmon]).

%% @doc Start the system monitor.
-spec start_link() -> startlink_ret().
start_link() ->
    gen_server:start_link({local, ?SYSMON}, ?MODULE, [], []).

add_handler() ->
    ok = emqx_config_handler:add_handler(?SYSMON_CONF_ROOT, ?MODULE),
    ok.

remove_handler() ->
    ok = emqx_config_handler:remove_handler(?SYSMON_CONF_ROOT),
    ok.

post_config_update(_, _Req, NewConf, OldConf, _AppEnvs) ->
    #{os := OS1, vm := VM1} = OldConf,
    #{os := OS2, vm := VM2} = NewConf,
    (VM1 =/= VM2) andalso ?MODULE:update(VM2),
    (OS1 =/= OS2) andalso emqx_os_mon:update(OS2),
    ok.

update(VM) ->
    erlang:send(?MODULE, {monitor_conf_update, VM}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    emqx_logger:set_proc_metadata(#{sysmon => true}),
    {ok, undefined, {continue, setup}}.

handle_continue(setup, undefined) ->
    init_system_monitor(),
    %% Monitor cluster partition event
    ekka:monitor(partition, fun handle_partition_event/1),
    NewState = start_timer(#{timer => undefined, events => []}),
    {noreply, NewState, hibernate}.

start_timer(State) ->
    State#{timer := emqx_utils:start_timer(timer:seconds(2), reset)}.

sysm_opts(VM) ->
    sysm_opts(maps:to_list(VM), []).
sysm_opts([], Acc) ->
    Acc;
sysm_opts([{_, disabled} | Opts], Acc) ->
    sysm_opts(Opts, Acc);
sysm_opts([{long_gc, Ms} | Opts], Acc) when is_integer(Ms) ->
    sysm_opts(Opts, [{long_gc, Ms} | Acc]);
sysm_opts([{long_schedule, Ms} | Opts], Acc) when is_integer(Ms) ->
    sysm_opts(Opts, [{long_schedule, Ms} | Acc]);
sysm_opts([{large_heap, Size} | Opts], Acc) when is_integer(Size) ->
    sysm_opts(Opts, [{large_heap, Size} | Acc]);
sysm_opts([{busy_port, true} | Opts], Acc) ->
    sysm_opts(Opts, [busy_port | Acc]);
sysm_opts([{busy_port, false} | Opts], Acc) ->
    sysm_opts(Opts, Acc);
sysm_opts([{busy_dist_port, true} | Opts], Acc) ->
    sysm_opts(Opts, [busy_dist_port | Acc]);
sysm_opts([{busy_dist_port, false} | Opts], Acc) ->
    sysm_opts(Opts, Acc);
sysm_opts([_Opt | Opts], Acc) ->
    sysm_opts(Opts, Acc).

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", req => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", req => Msg}),
    {noreply, State}.

handle_info({monitor, Pid, long_gc, Info}, State) ->
    suppress(
        {long_gc, Pid},
        fun() ->
            WarnMsg = io_lib:format("long_gc warning: pid = ~p", [Pid]),
            ?SLOG(warning, #{
                msg => "long_gc",
                info => Info,
                porcinfo => procinfo(Pid)
            }),
            safe_publish(long_gc, WarnMsg)
        end,
        State
    );
handle_info({monitor, Pid, long_schedule, Info}, State) when is_pid(Pid) ->
    suppress(
        {long_schedule, Pid},
        fun() ->
            WarnMsg = io_lib:format("long_schedule warning: pid = ~p", [Pid]),
            ?SLOG(warning, #{
                msg => "long_schedule",
                info => Info,
                procinfo => procinfo(Pid)
            }),
            safe_publish(long_schedule, WarnMsg)
        end,
        State
    );
handle_info({monitor, Port, long_schedule, Info}, State) when is_port(Port) ->
    suppress(
        {long_schedule, Port},
        fun() ->
            WarnMsg = io_lib:format("long_schedule warning: port = ~p", [Port]),
            ?SLOG(warning, #{
                msg => "long_schedule",
                info => Info,
                portinfo => portinfo(Port)
            }),
            safe_publish(long_schedule, WarnMsg)
        end,
        State
    );
handle_info({monitor, Pid, large_heap, Info}, State) ->
    suppress(
        {large_heap, Pid},
        fun() ->
            WarnMsg = io_lib:format("large_heap warning: pid = ~p", [Pid]),
            ?SLOG(warning, #{
                msg => "large_heap",
                info => Info,
                procinfo => procinfo(Pid)
            }),
            safe_publish(large_heap, WarnMsg)
        end,
        State
    );
handle_info({monitor, SusPid, busy_port, Port}, State) ->
    suppress(
        {busy_port, Port},
        fun() ->
            WarnMsg = io_lib:format("busy_port warning: suspid = ~p, port = ~p", [SusPid, Port]),
            ?SLOG(warning, #{
                msg => "busy_port",
                portinfo => portinfo(Port),
                procinfo => procinfo(SusPid)
            }),
            safe_publish(busy_port, WarnMsg)
        end,
        State
    );
handle_info({monitor, SusPid, busy_dist_port, Port}, State) ->
    suppress(
        {busy_dist_port, Port},
        fun() ->
            WarnMsg = io_lib:format("busy_dist_port warning: suspid = ~p, port = ~p", [SusPid, Port]),
            ?SLOG(warning, #{
                msg => "busy_dist_port",
                portinfo => portinfo(Port),
                procinfo => procinfo(SusPid)
            }),
            safe_publish(busy_dist_port, WarnMsg)
        end,
        State
    );
handle_info({timeout, _Ref, reset}, State) ->
    {noreply, State#{events := []}, hibernate};
handle_info({monitor_conf_update, VM}, State) ->
    init_system_monitor(VM),
    {noreply, State#{events := []}, hibernate};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, #{timer := TRef}) ->
    emqx_utils:cancel_timer(TRef),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

handle_partition_event({partition, {occurred, Node}}) ->
    Message = io_lib:format("Partition occurs at node ~ts", [Node]),
    emqx_alarm:activate(partition, #{occurred => Node}, Message);
handle_partition_event({partition, {healed, Node}}) ->
    Message = io_lib:format("Partition healed at node ~ts", [Node]),
    emqx_alarm:ensure_deactivated(partition, no_details, Message).

suppress(Key, SuccFun, State = #{events := Events}) ->
    case lists:member(Key, Events) of
        true ->
            {noreply, State};
        false ->
            _ = SuccFun(),
            {noreply, State#{events := [Key | Events]}}
    end.

procinfo(Pid) ->
    [{pid, Pid} | procinfo_l(emqx_vm:get_process_gc_info(Pid))] ++
        get_proc_lib_initial_call(Pid) ++
        procinfo_l(emqx_vm:get_process_info(Pid)).

procinfo_l(undefined) -> [];
procinfo_l(List) -> List.

get_proc_lib_initial_call(Pid) ->
    case proc_lib:initial_call(Pid) of
        false ->
            [];
        InitialCall ->
            [{proc_lib_initial_call, InitialCall}]
    end.

portinfo(Port) ->
    PortInfo =
        case is_port(Port) andalso erlang:port_info(Port) of
            L when is_list(L) -> L;
            _ -> []
        end,
    [{port, Port} | PortInfo].

safe_publish(Event, WarnMsg) ->
    Topic = emqx_topic:systop(lists:concat(['sysmon/', Event])),
    emqx_broker:safe_publish(sysmon_msg(Topic, iolist_to_binary(WarnMsg))).

sysmon_msg(Topic, Payload) ->
    Msg = emqx_message:make(?SYSMON, Topic, Payload),
    emqx_message:set_flag(sys, Msg).

init_system_monitor() ->
    VM = emqx:get_config([sysmon, vm]),
    init_system_monitor(VM).

init_system_monitor(VM) ->
    _ = erlang:system_monitor(self(), sysm_opts(VM)),
    ok.
