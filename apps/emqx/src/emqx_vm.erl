%%--------------------------------------------------------------------
%% Copyright (c) 2017-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_vm).

-export([
    schedulers/0,
    scheduler_usage/1,
    system_info_keys/0,
    get_system_info/0,
    get_system_info/1,
    get_memory/0,
    get_memory/2,
    mem_info/0,
    loads/0
]).

-export([
    process_info_keys/0,
    get_process_info/0,
    get_process_info/1,
    process_gc_info_keys/0,
    get_process_gc_info/0,
    get_process_gc_info/1,
    get_process_limit/0
]).

-export([
    get_ets_list/0,
    get_ets_info/0,
    get_ets_info/1,
    get_otp_version/0
]).

-export([cpu_util/0]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(UTIL_ALLOCATORS, [
    temp_alloc,
    eheap_alloc,
    binary_alloc,
    ets_alloc,
    driver_alloc,
    sl_alloc,
    ll_alloc,
    fix_alloc,
    literal_alloc,
    std_alloc
]).

-define(PROCESS_INFO_KEYS, [
    initial_call,
    current_stacktrace,
    registered_name,
    status,
    message_queue_len,
    group_leader,
    priority,
    trap_exit,
    reductions,
    %%binary,
    last_calls,
    catchlevel,
    trace,
    suspending,
    sequential_trace_token,
    error_handler
]).

-define(PROCESS_GC_KEYS, [
    memory,
    total_heap_size,
    heap_size,
    stack_size,
    min_heap_size
]).

-define(SYSTEM_INFO_KEYS, [
    allocated_areas,
    allocator,
    alloc_util_allocators,
    build_type,
    check_io,
    compat_rel,
    creation,
    debug_compiled,
    dist,
    dist_ctrl,
    driver_version,
    elib_malloc,
    dist_buf_busy_limit,
    %fullsweep_after, % included in garbage_collection
    garbage_collection,
    %global_heaps_size, % deprecated
    heap_sizes,
    heap_type,
    info,
    kernel_poll,
    loaded,
    logical_processors,
    logical_processors_available,
    logical_processors_online,
    machine,
    %min_heap_size, % included in garbage_collection
    %min_bin_vheap_size, % included in garbage_collection
    modified_timing_level,
    multi_scheduling,
    multi_scheduling_blockers,
    otp_release,
    port_count,
    process_count,
    process_limit,
    scheduler_bind_type,
    scheduler_bindings,
    scheduler_id,
    schedulers,
    schedulers_online,
    smp_support,
    system_version,
    system_architecture,
    threads,
    thread_pool_size,
    trace_control_word,
    update_cpu_info,
    version,
    wordsize
]).

-define(SOCKET_OPTS, [
    active,
    broadcast,
    buffer,
    delay_send,
    dontroute,
    exit_on_close,
    header,
    high_watermark,
    ipv6_v6only,
    keepalive,
    linger,
    low_watermark,
    mode,
    nodelay,
    packet,
    packet_size,
    priority,
    read_packets,
    recbuf,
    reuseaddr,
    send_timeout,
    send_timeout_close,
    sndbuf,
    tos
]).

schedulers() ->
    erlang:system_info(schedulers).

loads() ->
    [
        {load1, ftos(avg1() / 256)},
        {load5, ftos(avg5() / 256)},
        {load15, ftos(avg15() / 256)}
    ].

system_info_keys() -> ?SYSTEM_INFO_KEYS.

get_system_info() ->
    [{Key, format_system_info(Key, get_system_info(Key))} || Key <- ?SYSTEM_INFO_KEYS].

get_system_info(Key) ->
    try
        erlang:system_info(Key)
    catch
        error:badarg -> undefined
    end.

format_system_info(allocated_areas, List) ->
    [convert_allocated_areas(Value) || Value <- List];
format_system_info(allocator, {_, _, _, List}) ->
    List;
format_system_info(dist_ctrl, List) ->
    lists:map(
        fun({Node, Socket}) ->
            {ok, Stats} = inet:getstat(Socket),
            {Node, Stats}
        end,
        List
    );
format_system_info(driver_version, Value) ->
    list_to_binary(Value);
format_system_info(machine, Value) ->
    list_to_binary(Value);
format_system_info(otp_release, Value) ->
    list_to_binary(Value);
format_system_info(scheduler_bindings, Value) ->
    tuple_to_list(Value);
format_system_info(system_version, Value) ->
    list_to_binary(Value);
format_system_info(system_architecture, Value) ->
    list_to_binary(Value);
format_system_info(version, Value) ->
    list_to_binary(Value);
format_system_info(_, Value) ->
    Value.

convert_allocated_areas({Key, Value1, Value2}) ->
    {Key, [Value1, Value2]};
convert_allocated_areas({Key, Value}) ->
    {Key, Value}.

mem_info() ->
    Dataset = memsup:get_system_memory_data(),
    Total = proplists:get_value(total_memory, Dataset),
    Free = proplists:get_value(free_memory, Dataset),
    [{total_memory, Total}, {used_memory, Total - Free}].

ftos(F) ->
    io_lib:format("~.2f", [F / 1.0]).

%%%% erlang vm scheduler_usage  fun copied from recon
scheduler_usage(Interval) when is_integer(Interval) ->
    %% We start and stop the scheduler_wall_time system flag
    %% if it wasn't in place already. Usually setting the flag
    %% should have a CPU impact(make it higher) only when under low usage.
    FormerFlag = erlang:system_flag(scheduler_wall_time, true),
    First = erlang:statistics(scheduler_wall_time),
    timer:sleep(Interval),
    Last = erlang:statistics(scheduler_wall_time),
    erlang:system_flag(scheduler_wall_time, FormerFlag),
    scheduler_usage_diff(First, Last).

scheduler_usage_diff(First, Last) ->
    lists:map(
        fun({{I, A0, T0}, {I, A1, T1}}) ->
            {I, (A1 - A0) / (T1 - T0)}
        end,
        lists:zip(lists:sort(First), lists:sort(Last))
    ).

get_memory() ->
    get_memory_once(current) ++ erlang:memory().

get_memory(Ks, Keyword) when is_list(Ks) ->
    Ms = get_memory_once(Keyword) ++ erlang:memory(),
    [M || M = {K, _} <- Ms, lists:member(K, Ks)];
get_memory(used, Keyword) ->
    lists:sum(
        lists:map(
            fun({_, Prop}) ->
                container_size(Prop, Keyword, blocks_size)
            end,
            util_alloc()
        )
    );
get_memory(allocated, Keyword) ->
    lists:sum(
        lists:map(
            fun({_, Prop}) ->
                container_size(Prop, Keyword, carriers_size)
            end,
            util_alloc()
        )
    );
get_memory(unused, Keyword) ->
    Ms = get_memory_once(Keyword),
    proplists:get_value(allocated, Ms) - proplists:get_value(used, Ms);
get_memory(usage, Keyword) ->
    Ms = get_memory_once(Keyword),
    proplists:get_value(used, Ms) / proplists:get_value(allocated, Ms).

%% @private A more quickly function to calculate memory
get_memory_once(Keyword) ->
    Calc = fun({_, Prop}, {N1, N2}) ->
        {
            N1 + container_size(Prop, Keyword, blocks_size),
            N2 + container_size(Prop, Keyword, carriers_size)
        }
    end,
    {Used, Allocated} = lists:foldl(Calc, {0, 0}, util_alloc()),
    [
        {used, Used},
        {allocated, Allocated},
        {unused, Allocated - Used},
        {usage, Used / Allocated}
    ].

util_alloc() ->
    alloc(?UTIL_ALLOCATORS).

alloc(Type) ->
    [
        {{T, Instance}, Props}
     || {{T, Instance}, Props} <- recon_alloc:allocators(), lists:member(T, Type)
    ].

container_size(Prop, Keyword, Container) ->
    Sbcs = container_value(Prop, Keyword, sbcs, Container),
    Mbcs = container_value(Prop, Keyword, mbcs, Container),
    Sbcs + Mbcs.

container_value(Prop, Keyword, Type, Container) when is_atom(Keyword) ->
    container_value(Prop, 2, Type, Container);
container_value(Props, Pos, mbcs = Type, Container) when is_integer(Pos) ->
    Pool =
        case proplists:get_value(mbcs_pool, Props) of
            PoolProps when PoolProps =/= undefined ->
                element(Pos, lists:keyfind(Container, 1, PoolProps));
            _ ->
                0
        end,
    TypeProps = proplists:get_value(Type, Props),
    Pool + element(Pos, lists:keyfind(Container, 1, TypeProps));
container_value(Props, Pos, Type, Container) ->
    TypeProps = proplists:get_value(Type, Props),
    element(Pos, lists:keyfind(Container, 1, TypeProps)).

process_info_keys() ->
    ?PROCESS_INFO_KEYS.

get_process_info() ->
    get_process_info(self()).
get_process_info(Pid) when is_pid(Pid) ->
    process_info(Pid, ?PROCESS_INFO_KEYS).

process_gc_info_keys() ->
    ?PROCESS_GC_KEYS.

get_process_gc_info() ->
    get_process_gc_info(self()).
get_process_gc_info(Pid) when is_pid(Pid) ->
    process_info(Pid, ?PROCESS_GC_KEYS).

get_process_limit() ->
    erlang:system_info(process_limit).

get_ets_list() ->
    ets:all().

get_ets_info() ->
    [get_ets_info(Tab) || Tab <- ets:all()].

get_ets_info(Tab) ->
    case ets:info(Tab) of
        undefined ->
            [];
        Entries when is_list(Entries) ->
            mapping(Entries)
    end.

mapping(Entries) ->
    mapping(Entries, []).
mapping([], Acc) ->
    Acc;
mapping([{owner, V} | Entries], Acc) when is_pid(V) ->
    OwnerInfo = process_info(V),
    Owner = proplists:get_value(registered_name, OwnerInfo, undefined),
    mapping(Entries, [{owner, Owner} | Acc]);
mapping([{Key, Value} | Entries], Acc) ->
    mapping(Entries, [{Key, Value} | Acc]).

avg1() ->
    compat_windows(fun cpu_sup:avg1/0).

avg5() ->
    compat_windows(fun cpu_sup:avg5/0).

avg15() ->
    compat_windows(fun cpu_sup:avg15/0).

cpu_util() ->
    compat_windows(fun cpu_sup:util/0).

compat_windows(Fun) ->
    case os:type() of
        {win32, nt} ->
            0;
        _Type ->
            case catch Fun() of
                Val when is_number(Val) -> Val;
                _Error -> 0
            end
    end.

%% @doc Return on which Eralng/OTP the current vm is running.
%% NOTE: This API reads a file, do not use it in critical code paths.
get_otp_version() ->
    read_otp_version().

read_otp_version() ->
    ReleasesDir = filename:join([code:root_dir(), "releases"]),
    Filename = filename:join([ReleasesDir, emqx_app:get_release(), "BUILD_INFO"]),
    case file:read_file(Filename) of
        {ok, BuildInfo} ->
            %% running on EMQX release
            {ok, Fields} = hocon:binary(BuildInfo),
            hocon_maps:get("erlang", Fields);
        {error, enoent} ->
            %% running tests etc.
            OtpMajor = erlang:system_info(otp_release),
            OtpVsnFile = filename:join([ReleasesDir, OtpMajor, "OTP_VERSION"]),
            {ok, Vsn} = file:read_file(OtpVsnFile),
            Vsn
    end.
