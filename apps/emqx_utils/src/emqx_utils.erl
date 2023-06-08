%%--------------------------------------------------------------------
%% Copyright (c) 2017-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_utils).

-compile(inline).
%% [TODO] Cleanup so the instruction below is not necessary.
-elvis([{elvis_style, god_modules, disable}]).

-export([
    merge_opts/2,
    maybe_apply/2,
    compose/1,
    compose/2,
    cons/2,
    run_fold/3,
    pipeline/3,
    start_timer/2,
    start_timer/3,
    cancel_timer/1,
    drain_deliver/0,
    drain_deliver/1,
    drain_down/1,
    check_oom/1,
    check_oom/2,
    tune_heap_size/1,
    proc_name/2,
    proc_stats/0,
    proc_stats/1,
    rand_seed/0,
    now_to_secs/1,
    now_to_ms/1,
    index_of/2,
    maybe_parse_ip/1,
    ipv6_probe/1,
    gen_id/0,
    gen_id/1,
    explain_posix/1,
    pmap/2,
    pmap/3,
    readable_error_msg/1,
    safe_to_existing_atom/1,
    safe_to_existing_atom/2,
    pub_props_to_packet/1,
    safe_filename/1,
    diff_lists/3
]).

-export([
    bin_to_hexstr/2,
    hexstr_to_bin/1
]).

-export([
    nolink_apply/1,
    nolink_apply/2
]).

-export([clamp/3, redact/1, redact/2, is_redacted/2, is_redacted/3]).

-type maybe(T) :: undefined | T.

-dialyzer({nowarn_function, [nolink_apply/2]}).

-define(SHORT, 8).

-define(DEFAULT_PMAP_TIMEOUT, 5000).

%% @doc Parse v4 or v6 string format address to tuple.
%% `Host' itself is returned if it's not an ip string.
maybe_parse_ip(Host) ->
    case inet:parse_address(Host) of
        {ok, Addr} when is_tuple(Addr) -> Addr;
        {error, einval} -> Host
    end.

%% @doc Add `ipv6_probe' socket option if it's supported.
%% gen_tcp:ipv6_probe() -> true. is added to EMQ's OTP forks
ipv6_probe(Opts) ->
    case erlang:function_exported(gen_tcp, ipv6_probe, 0) of
        true -> [{ipv6_probe, true} | Opts];
        false -> Opts
    end.

%% @doc Merge options
-spec merge_opts(Opts, Opts) -> Opts when Opts :: proplists:proplist().
merge_opts(Defaults, Options) ->
    lists:foldl(
        fun
            ({Opt, Val}, Acc) ->
                lists:keystore(Opt, 1, Acc, {Opt, Val});
            (Opt, Acc) ->
                lists:usort([Opt | Acc])
        end,
        Defaults,
        Options
    ).

%% @doc Apply a function to a maybe argument.
-spec maybe_apply(fun((maybe(A)) -> maybe(A)), maybe(A)) ->
    maybe(A)
when
    A :: any().
maybe_apply(_Fun, undefined) ->
    undefined;
maybe_apply(Fun, Arg) when is_function(Fun) ->
    erlang:apply(Fun, [Arg]).

-spec compose(list(F)) -> G when
    F :: fun((any()) -> any()),
    G :: fun((any()) -> any()).
compose([F | More]) -> compose(F, More).

-spec compose(F, G | [Gs]) -> C when
    F :: fun((X1) -> X2),
    G :: fun((X2) -> X3),
    Gs :: [fun((Xn) -> Xn1)],
    C :: fun((X1) -> Xm),
    X3 :: any(),
    Xn :: any(),
    Xn1 :: any(),
    Xm :: any().
compose(F, G) when is_function(G) -> fun(X) -> G(F(X)) end;
compose(F, [G]) -> compose(F, G);
compose(F, [G | More]) -> compose(compose(F, G), More).

-spec cons(X, [X]) -> [X, ...].
cons(Head, Tail) ->
    [Head | Tail].

%% @doc RunFold
run_fold([], Acc, _State) ->
    Acc;
run_fold([Fun | More], Acc, State) ->
    run_fold(More, Fun(Acc, State), State).

%% @doc Pipeline
pipeline([], Input, State) ->
    {ok, Input, State};
pipeline([Fun | More], Input, State) ->
    case apply_fun(Fun, Input, State) of
        ok -> pipeline(More, Input, State);
        {ok, NState} -> pipeline(More, Input, NState);
        {ok, Output, NState} -> pipeline(More, Output, NState);
        {error, Reason} -> {error, Reason, State};
        {error, Reason, NState} -> {error, Reason, NState}
    end.

-compile({inline, [apply_fun/3]}).
apply_fun(Fun, Input, State) ->
    case erlang:fun_info(Fun, arity) of
        {arity, 1} -> Fun(Input);
        {arity, 2} -> Fun(Input, State)
    end.

-spec start_timer(integer() | atom(), term()) -> maybe(reference()).
start_timer(Interval, Msg) ->
    start_timer(Interval, self(), Msg).

-spec start_timer(integer() | atom(), pid() | atom(), term()) -> maybe(reference()).
start_timer(Interval, Dest, Msg) when is_number(Interval) ->
    erlang:start_timer(erlang:ceil(Interval), Dest, Msg);
start_timer(_Atom, _Dest, _Msg) ->
    undefined.

-spec cancel_timer(maybe(reference())) -> ok.
cancel_timer(Timer) when is_reference(Timer) ->
    case erlang:cancel_timer(Timer) of
        false ->
            receive
                {timeout, Timer, _} -> ok
            after 0 -> ok
            end;
        _ ->
            ok
    end;
cancel_timer(_) ->
    ok.

%% @doc Drain delivers
drain_deliver() ->
    drain_deliver(-1).

drain_deliver(N) when is_integer(N) ->
    drain_deliver(N, []).

drain_deliver(0, Acc) ->
    lists:reverse(Acc);
drain_deliver(N, Acc) ->
    receive
        Deliver = {deliver, _Topic, _Msg} ->
            drain_deliver(N - 1, [Deliver | Acc])
    after 0 ->
        lists:reverse(Acc)
    end.

%% @doc Drain process 'DOWN' events.
-spec drain_down(pos_integer()) -> list(pid()).
drain_down(Cnt) when Cnt > 0 ->
    drain_down(Cnt, []).

drain_down(0, Acc) ->
    lists:reverse(Acc);
drain_down(Cnt, Acc) ->
    receive
        {'DOWN', _MRef, process, Pid, _Reason} ->
            drain_down(Cnt - 1, [Pid | Acc])
    after 0 ->
        lists:reverse(Acc)
    end.

%% @doc Check process's mailbox and heapsize against OOM policy,
%% return `ok | {shutdown, Reason}' accordingly.
%% `ok': There is nothing out of the ordinary.
%% `shutdown': Some numbers (message queue length hit the limit),
%%             hence shutdown for greater good (system stability).
%% [FIXME] cross-dependency on `emqx_types`.
-spec check_oom(emqx_types:oom_policy()) -> ok | {shutdown, term()}.
check_oom(Policy) ->
    check_oom(self(), Policy).

-spec check_oom(pid(), emqx_types:oom_policy()) -> ok | {shutdown, term()}.
check_oom(_Pid, #{enable := false}) ->
    ok;
check_oom(Pid, #{
    max_mailbox_size := MaxQLen,
    max_heap_size := MaxHeapSize
}) ->
    case process_info(Pid, [message_queue_len, total_heap_size]) of
        undefined ->
            ok;
        [{message_queue_len, QLen}, {total_heap_size, HeapSize}] ->
            do_check_oom([
                {QLen, MaxQLen, message_queue_too_long},
                {HeapSize, MaxHeapSize, proc_heap_too_large}
            ])
    end.

do_check_oom([]) ->
    ok;
do_check_oom([{Val, Max, Reason} | Rest]) ->
    case is_integer(Max) andalso (0 < Max) andalso (Max < Val) of
        true -> {shutdown, #{reason => Reason, value => Val, max => Max}};
        false -> do_check_oom(Rest)
    end.

tune_heap_size(#{enable := false}) ->
    ok;
%% If the max_heap_size is set to zero, the limit is disabled.
tune_heap_size(#{max_heap_size := MaxHeapSize}) when MaxHeapSize > 0 ->
    MaxSize =
        case erlang:system_info(wordsize) of
            % arch_64
            8 ->
                (1 bsl 59) - 1;
            % arch_32
            4 ->
                (1 bsl 27) - 1
        end,
    OverflowedSize =
        case erlang:trunc(MaxHeapSize * 1.5) of
            SZ when SZ > MaxSize -> MaxSize;
            SZ -> SZ
        end,
    erlang:process_flag(max_heap_size, #{
        size => OverflowedSize,
        kill => true,
        error_logger => true
    }).

-spec proc_name(atom(), pos_integer()) -> atom().
proc_name(Mod, Id) ->
    list_to_atom(lists:concat([Mod, "_", Id])).

%% Get Proc's Stats.
%% [FIXME] cross-dependency on `emqx_types`.
-spec proc_stats() -> emqx_types:stats().
proc_stats() -> proc_stats(self()).

-spec proc_stats(pid()) -> emqx_types:stats().
proc_stats(Pid) ->
    case
        process_info(Pid, [
            message_queue_len,
            heap_size,
            total_heap_size,
            reductions,
            memory
        ])
    of
        undefined -> [];
        [{message_queue_len, Len} | ProcStats] -> [{mailbox_len, Len} | ProcStats]
    end.

rand_seed() ->
    rand:seed(exsplus, erlang:timestamp()).

-spec now_to_secs(erlang:timestamp()) -> pos_integer().
now_to_secs({MegaSecs, Secs, _MicroSecs}) ->
    MegaSecs * 1000000 + Secs.

-spec now_to_ms(erlang:timestamp()) -> pos_integer().
now_to_ms({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000 + round(MicroSecs / 1000).

%% lists:index_of/2
index_of(E, L) ->
    index_of(E, 1, L).

index_of(_E, _I, []) ->
    error(badarg);
index_of(E, I, [E | _]) ->
    I;
index_of(E, I, [_ | L]) ->
    index_of(E, I + 1, L).

-spec bin_to_hexstr(binary(), lower | upper) -> binary().
bin_to_hexstr(B, upper) when is_binary(B) ->
    <<<<(int2hexchar(H, upper)), (int2hexchar(L, upper))>> || <<H:4, L:4>> <= B>>;
bin_to_hexstr(B, lower) when is_binary(B) ->
    <<<<(int2hexchar(H, lower)), (int2hexchar(L, lower))>> || <<H:4, L:4>> <= B>>.

int2hexchar(I, _) when I >= 0 andalso I < 10 -> I + $0;
int2hexchar(I, upper) -> I - 10 + $A;
int2hexchar(I, lower) -> I - 10 + $a.

-spec hexstr_to_bin(binary()) -> binary().
hexstr_to_bin(B) when is_binary(B) ->
    hexstr_to_bin(B, erlang:bit_size(B)).

hexstr_to_bin(B, Size) when is_binary(B) ->
    case Size rem 16 of
        0 ->
            make_binary(B);
        8 ->
            make_binary(<<"0", B/binary>>);
        _ ->
            throw({unsupport_hex_string, B, Size})
    end.

make_binary(B) -> <<<<(hexchar2int(H) * 16 + hexchar2int(L))>> || <<H:8, L:8>> <= B>>.

hexchar2int(I) when I >= $0 andalso I =< $9 -> I - $0;
hexchar2int(I) when I >= $A andalso I =< $F -> I - $A + 10;
hexchar2int(I) when I >= $a andalso I =< $f -> I - $a + 10.

-spec gen_id() -> list().
gen_id() ->
    gen_id(?SHORT).

-spec gen_id(integer()) -> list().
gen_id(Len) ->
    BitLen = Len * 4,
    <<R:BitLen>> = crypto:strong_rand_bytes(Len div 2),
    int_to_hex(R, Len).

-spec clamp(number(), number(), number()) -> number().
clamp(Val, Min, _Max) when Val < Min -> Min;
clamp(Val, _Min, Max) when Val > Max -> Max;
clamp(Val, _Min, _Max) -> Val.

%% @doc https://www.erlang.org/doc/man/file.html#posix-error-codes
explain_posix(eacces) -> "Permission denied";
explain_posix(eagain) -> "Resource temporarily unavailable";
explain_posix(ebadf) -> "Bad file number";
explain_posix(ebusy) -> "File busy";
explain_posix(edquot) -> "Disk quota exceeded";
explain_posix(eexist) -> "File already exists";
explain_posix(efault) -> "Bad address in system call argument";
explain_posix(efbig) -> "File too large";
explain_posix(eintr) -> "Interrupted system call";
explain_posix(einval) -> "Invalid argument argument file/socket";
explain_posix(eio) -> "I/O error";
explain_posix(eisdir) -> "Illegal operation on a directory";
explain_posix(eloop) -> "Too many levels of symbolic links";
explain_posix(emfile) -> "Too many open files";
explain_posix(emlink) -> "Too many links";
explain_posix(enametoolong) -> "Filename too long";
explain_posix(enfile) -> "File table overflow";
explain_posix(enodev) -> "No such device";
explain_posix(enoent) -> "No such file or directory";
explain_posix(enomem) -> "Not enough memory";
explain_posix(enospc) -> "No space left on device";
explain_posix(enotblk) -> "Block device required";
explain_posix(enotdir) -> "Not a directory";
explain_posix(enotsup) -> "Operation not supported";
explain_posix(enxio) -> "No such device or address";
explain_posix(eperm) -> "Not owner";
explain_posix(epipe) -> "Broken pipe";
explain_posix(erofs) -> "Read-only file system";
explain_posix(espipe) -> "Invalid seek";
explain_posix(esrch) -> "No such process";
explain_posix(estale) -> "Stale remote file handle";
explain_posix(exdev) -> "Cross-domain link";
explain_posix(NotPosix) -> NotPosix.

%% @doc Like lists:map/2, only the callback function is evaluated
%% concurrently.
-spec pmap(fun((A) -> B), list(A)) -> list(B).
pmap(Fun, List) when is_function(Fun, 1), is_list(List) ->
    pmap(Fun, List, ?DEFAULT_PMAP_TIMEOUT).

-spec pmap(fun((A) -> B), list(A), timeout()) -> list(B).
pmap(Fun, List, Timeout) when
    is_function(Fun, 1), is_list(List), is_integer(Timeout), Timeout >= 0
->
    nolink_apply(fun() -> do_parallel_map(Fun, List) end, Timeout).

%% @doc Delegate a function to a worker process.
%% The function may spawn_link other processes but we do not
%% want the caller process to be linked.
%% This is done by isolating the possible link with a not-linked
%% middleman process.
nolink_apply(Fun) -> nolink_apply(Fun, infinity).

%% @doc Same as `nolink_apply/1', with a timeout.
-spec nolink_apply(function(), timer:timeout()) -> term().
nolink_apply(Fun, Timeout) when is_function(Fun, 0) ->
    Caller = self(),
    ResRef = make_ref(),
    Middleman = erlang:spawn(
        fun() ->
            process_flag(trap_exit, true),
            CallerMRef = erlang:monitor(process, Caller),
            Worker = erlang:spawn_link(
                fun() ->
                    Res =
                        try
                            {normal, Fun()}
                        catch
                            C:E:S ->
                                {exception, {C, E, S}}
                        end,
                    _ = erlang:send(Caller, {ResRef, Res}),
                    exit(normal)
                end
            ),
            receive
                {'DOWN', CallerMRef, process, _, _} ->
                    %% For whatever reason, if the caller is dead,
                    %% there is no reason to continue
                    exit(Worker, kill),
                    exit(normal);
                {'EXIT', Worker, normal} ->
                    exit(normal);
                {'EXIT', Worker, Reason} ->
                    %% worker exited with some reason other than 'normal'
                    _ = erlang:send(Caller, {ResRef, {'EXIT', Reason}}),
                    exit(normal)
            end
        end
    ),
    receive
        {ResRef, {normal, Result}} ->
            Result;
        {ResRef, {exception, {C, E, S}}} ->
            erlang:raise(C, E, S);
        {ResRef, {'EXIT', Reason}} ->
            exit(Reason)
    after Timeout ->
        exit(Middleman, kill),
        exit(timeout)
    end.

safe_to_existing_atom(In) ->
    safe_to_existing_atom(In, utf8).

safe_to_existing_atom(Bin, Encoding) when is_binary(Bin) ->
    try_to_existing_atom(fun erlang:binary_to_existing_atom/2, Bin, Encoding);
safe_to_existing_atom(List, Encoding) when is_list(List) ->
    try_to_existing_atom(fun(In, _) -> erlang:list_to_existing_atom(In) end, List, Encoding);
safe_to_existing_atom(Atom, _Encoding) when is_atom(Atom) ->
    {ok, Atom};
safe_to_existing_atom(_Any, _Encoding) ->
    {error, invalid_type}.

%%------------------------------------------------------------------------------
%% Internal Functions
%%------------------------------------------------------------------------------

do_parallel_map(Fun, List) ->
    Parent = self(),
    PidList = lists:map(
        fun(Item) ->
            erlang:spawn_link(
                fun() ->
                    Res =
                        try
                            {normal, Fun(Item)}
                        catch
                            C:E:St ->
                                {exception, {C, E, St}}
                        end,
                    Parent ! {self(), Res}
                end
            )
        end,
        List
    ),
    lists:foldr(
        fun(Pid, Acc) ->
            receive
                {Pid, {normal, Result}} ->
                    [Result | Acc];
                {Pid, {exception, {C, E, St}}} ->
                    erlang:raise(C, E, St)
            end
        end,
        [],
        PidList
    ).

int_to_hex(I, N) when is_integer(I), I >= 0 ->
    int_to_hex([], I, 1, N).

int_to_hex(L, I, Count, N) when
    I < 16
->
    pad([int_to_hex(I) | L], N - Count);
int_to_hex(L, I, Count, N) ->
    int_to_hex([int_to_hex(I rem 16) | L], I div 16, Count + 1, N).

int_to_hex(I) when 0 =< I, I =< 9 ->
    I + $0;
int_to_hex(I) when 10 =< I, I =< 15 ->
    (I - 10) + $a.

pad(L, 0) ->
    L;
pad(L, Count) ->
    pad([$0 | L], Count - 1).

readable_error_msg(Msg) when is_binary(Msg) -> Msg;
readable_error_msg(Error) ->
    case io_lib:printable_unicode_list(Error) of
        true ->
            unicode:characters_to_binary(Error, utf8);
        false ->
            case emqx_hocon:format_error(Error, #{no_stacktrace => true}) of
                {ok, Msg} ->
                    Msg;
                false ->
                    to_hr_error(Error)
            end
    end.

to_hr_error(nxdomain) ->
    <<"Could not resolve host">>;
to_hr_error(econnrefused) ->
    <<"Connection refused">>;
to_hr_error({unauthorized_client, _}) ->
    <<"Unauthorized client">>;
to_hr_error({not_authorized, _}) ->
    <<"Not authorized">>;
to_hr_error({malformed_username_or_password, _}) ->
    <<"Bad username or password">>;
to_hr_error(Error) ->
    iolist_to_binary(io_lib:format("~0p", [Error])).

try_to_existing_atom(Convert, Data, Encoding) ->
    try Convert(Data, Encoding) of
        Atom ->
            {ok, Atom}
    catch
        _:Reason -> {error, Reason}
    end.

is_sensitive_key(token) -> true;
is_sensitive_key("token") -> true;
is_sensitive_key(<<"token">>) -> true;
is_sensitive_key(authorization) -> true;
is_sensitive_key("authorization") -> true;
is_sensitive_key(<<"authorization">>) -> true;
is_sensitive_key(password) -> true;
is_sensitive_key("password") -> true;
is_sensitive_key(<<"password">>) -> true;
is_sensitive_key(secret) -> true;
is_sensitive_key("secret") -> true;
is_sensitive_key(<<"secret">>) -> true;
is_sensitive_key(secret_key) -> true;
is_sensitive_key("secret_key") -> true;
is_sensitive_key(<<"secret_key">>) -> true;
is_sensitive_key(security_token) -> true;
is_sensitive_key("security_token") -> true;
is_sensitive_key(<<"security_token">>) -> true;
is_sensitive_key(aws_secret_access_key) -> true;
is_sensitive_key("aws_secret_access_key") -> true;
is_sensitive_key(<<"aws_secret_access_key">>) -> true;
is_sensitive_key(_) -> false.

redact(Term) ->
    do_redact(Term, fun is_sensitive_key/1).

redact(Term, Checker) ->
    do_redact(Term, fun(V) ->
        is_sensitive_key(V) orelse Checker(V)
    end).

do_redact(L, Checker) when is_list(L) ->
    lists:map(fun(E) -> do_redact(E, Checker) end, L);
do_redact(M, Checker) when is_map(M) ->
    maps:map(
        fun(K, V) ->
            do_redact(K, V, Checker)
        end,
        M
    );
do_redact({Key, Value}, Checker) ->
    case Checker(Key) of
        true ->
            {Key, redact_v(Value)};
        false ->
            {do_redact(Key, Checker), do_redact(Value, Checker)}
    end;
do_redact(T, Checker) when is_tuple(T) ->
    Elements = erlang:tuple_to_list(T),
    Redact = do_redact(Elements, Checker),
    erlang:list_to_tuple(Redact);
do_redact(Any, _Checker) ->
    Any.

do_redact(K, V, Checker) ->
    case Checker(K) of
        true ->
            redact_v(V);
        false ->
            do_redact(V, Checker)
    end.

-define(REDACT_VAL, "******").
redact_v(V) when is_binary(V) -> <<?REDACT_VAL>>;
%% The HOCON schema system may generate sensitive values with this format
redact_v([{str, Bin}]) when is_binary(Bin) ->
    [{str, <<?REDACT_VAL>>}];
redact_v(_V) ->
    ?REDACT_VAL.

is_redacted(K, V) ->
    do_is_redacted(K, V, fun is_sensitive_key/1).

is_redacted(K, V, Fun) ->
    do_is_redacted(K, V, fun(E) ->
        is_sensitive_key(E) orelse Fun(E)
    end).

do_is_redacted(K, ?REDACT_VAL, Fun) ->
    Fun(K);
do_is_redacted(K, <<?REDACT_VAL>>, Fun) ->
    Fun(K);
do_is_redacted(_K, _V, _Fun) ->
    false.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

ipv6_probe_test() ->
    try gen_tcp:ipv6_probe() of
        true ->
            ?assertEqual([{ipv6_probe, true}], ipv6_probe([]))
    catch
        _:_ ->
            ok
    end.

redact_test_() ->
    Case = fun(Type, KeyT) ->
        Key =
            case Type of
                atom -> KeyT;
                string -> erlang:atom_to_list(KeyT);
                binary -> erlang:atom_to_binary(KeyT)
            end,

        ?assert(is_sensitive_key(Key)),

        %% direct
        ?assertEqual({Key, ?REDACT_VAL}, redact({Key, foo})),
        ?assertEqual(#{Key => ?REDACT_VAL}, redact(#{Key => foo})),
        ?assertEqual({Key, Key, Key}, redact({Key, Key, Key})),
        ?assertEqual({[{Key, ?REDACT_VAL}], bar}, redact({[{Key, foo}], bar})),

        %% 1 level nested
        ?assertEqual([{Key, ?REDACT_VAL}], redact([{Key, foo}])),
        ?assertEqual([#{Key => ?REDACT_VAL}], redact([#{Key => foo}])),

        %% 2 level nested
        ?assertEqual(#{opts => [{Key, ?REDACT_VAL}]}, redact(#{opts => [{Key, foo}]})),
        ?assertEqual(#{opts => #{Key => ?REDACT_VAL}}, redact(#{opts => #{Key => foo}})),
        ?assertEqual({opts, [{Key, ?REDACT_VAL}]}, redact({opts, [{Key, foo}]})),

        %% 3 level nested
        ?assertEqual([#{opts => [{Key, ?REDACT_VAL}]}], redact([#{opts => [{Key, foo}]}])),
        ?assertEqual([{opts, [{Key, ?REDACT_VAL}]}], redact([{opts, [{Key, foo}]}])),
        ?assertEqual([{opts, [#{Key => ?REDACT_VAL}]}], redact([{opts, [#{Key => foo}]}]))
    end,

    Types = [atom, string, binary],
    Keys = [
        token,
        password,
        secret
    ],
    [{case_name(Type, Key), fun() -> Case(Type, Key) end} || Key <- Keys, Type <- Types].

redact2_test_() ->
    Case = fun(Key, Checker) ->
        ?assertEqual({Key, ?REDACT_VAL}, redact({Key, foo}, Checker)),
        ?assertEqual(#{Key => ?REDACT_VAL}, redact(#{Key => foo}, Checker)),
        ?assertEqual({Key, Key, Key}, redact({Key, Key, Key}, Checker)),
        ?assertEqual({[{Key, ?REDACT_VAL}], bar}, redact({[{Key, foo}], bar}, Checker))
    end,

    Checker = fun(E) -> E =:= passcode end,

    Keys = [secret, passcode],
    [{case_name(atom, Key), fun() -> Case(Key, Checker) end} || Key <- Keys].

case_name(Type, Key) ->
    lists:concat([Type, "-", Key]).

-endif.

pub_props_to_packet(Properties) ->
    F = fun
        ('User-Property', M) ->
            case is_map(M) andalso map_size(M) > 0 of
                true -> {true, maps:to_list(M)};
                false -> false
            end;
        ('User-Property-Pairs', _) ->
            false;
        (_, _) ->
            true
    end,
    maps:filtermap(F, Properties).

%% fix filename by replacing characters which could be invalid on some filesystems
%% with safe ones
-spec safe_filename(binary() | unicode:chardata()) -> binary() | [unicode:chardata()].
safe_filename(Filename) when is_binary(Filename) ->
    binary:replace(Filename, <<":">>, <<"-">>, [global]);
safe_filename(Filename) when is_list(Filename) ->
    lists:flatten(string:replace(Filename, ":", "-", all)).

%% @doc Compares two lists of maps and returns the differences between them in a
%% map containing four keys – 'removed', 'added', 'identical', and 'changed' –
%% each holding a list of maps. Elements are compared using key function KeyFunc
%% to extract the comparison key used for matching.
%%
%% The return value is a map with the following keys and the list of maps as its values:
%% * 'removed' – a list of maps that were present in the Old list, but not found in the New list.
%% * 'added' – a list of maps that were present in the New list, but not found in the Old list.
%% * 'identical' – a list of maps that were present in both lists and have the same comparison key value.
%% * 'changed' – a list of pairs of maps representing the changes between maps present in the New and Old lists.
%% The first map in the pair represents the map in the Old list, and the second map
%% represents the potential modification in the New list.

%% The KeyFunc parameter is a function that extracts the comparison key used
%% for matching from each map. The function should return a comparable term,
%% such as an atom, a number, or a string. This is used to determine if each
%% element is the same in both lists.

-spec diff_lists(list(T), list(T), Func) ->
    #{
        added := list(T),
        identical := list(T),
        removed := list(T),
        changed := list({Old :: T, New :: T})
    }
when
    Func :: fun((T) -> any()),
    T :: any().

diff_lists(New, Old, KeyFunc) when is_list(New) andalso is_list(Old) ->
    Removed =
        lists:foldl(
            fun(E, RemovedAcc) ->
                case search(KeyFunc(E), KeyFunc, New) of
                    false -> [E | RemovedAcc];
                    _ -> RemovedAcc
                end
            end,
            [],
            Old
        ),
    {Added, Identical, Changed} =
        lists:foldl(
            fun(E, Acc) ->
                {Added0, Identical0, Changed0} = Acc,
                case search(KeyFunc(E), KeyFunc, Old) of
                    false ->
                        {[E | Added0], Identical0, Changed0};
                    E ->
                        {Added0, [E | Identical0], Changed0};
                    E1 ->
                        {Added0, Identical0, [{E1, E} | Changed0]}
                end
            end,
            {[], [], []},
            New
        ),
    #{
        removed => lists:reverse(Removed),
        added => lists:reverse(Added),
        identical => lists:reverse(Identical),
        changed => lists:reverse(Changed)
    }.

search(_ExpectValue, _KeyFunc, []) ->
    false;
search(ExpectValue, KeyFunc, [Item | List]) ->
    case KeyFunc(Item) =:= ExpectValue of
        true -> Item;
        false -> search(ExpectValue, KeyFunc, List)
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

diff_lists_test() ->
    KeyFunc = fun(#{name := Name}) -> Name end,
    ?assertEqual(
        #{
            removed => [],
            added => [],
            identical => [],
            changed => []
        },
        diff_lists([], [], KeyFunc)
    ),
    %% test removed list
    ?assertEqual(
        #{
            removed => [#{name => a, value => 1}],
            added => [],
            identical => [],
            changed => []
        },
        diff_lists([], [#{name => a, value => 1}], KeyFunc)
    ),
    %% test added list
    ?assertEqual(
        #{
            removed => [],
            added => [#{name => a, value => 1}],
            identical => [],
            changed => []
        },
        diff_lists([#{name => a, value => 1}], [], KeyFunc)
    ),
    %% test identical list
    ?assertEqual(
        #{
            removed => [],
            added => [],
            identical => [#{name => a, value => 1}],
            changed => []
        },
        diff_lists([#{name => a, value => 1}], [#{name => a, value => 1}], KeyFunc)
    ),
    Old = [
        #{name => a, value => 1},
        #{name => b, value => 4},
        #{name => e, value => 2},
        #{name => d, value => 4}
    ],
    New = [
        #{name => a, value => 1},
        #{name => b, value => 2},
        #{name => e, value => 2},
        #{name => c, value => 3}
    ],
    Diff = diff_lists(New, Old, KeyFunc),
    ?assertEqual(
        #{
            added => [
                #{name => c, value => 3}
            ],
            identical => [
                #{name => a, value => 1},
                #{name => e, value => 2}
            ],
            removed => [
                #{name => d, value => 4}
            ],
            changed => [{#{name => b, value => 4}, #{name => b, value => 2}}]
        },
        Diff
    ),
    ok.

-endif.
