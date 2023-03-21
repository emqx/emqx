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

-module(emqx_misc).

-compile(inline).

-include("types.hrl").
-include("logger.hrl").

-elvis([{elvis_style, god_modules, disable}]).

-export([ merge_opts/2
        , maybe_apply/2
        , maybe_mute_rpc_log/0
        , safe_io_device/0
        , compose/1
        , compose/2
        , run_fold/3
        , pipeline/3
        , start_timer/2
        , start_timer/3
        , cancel_timer/1
        , drain_deliver/0
        , drain_deliver/1
        , drain_down/1
        , check_oom/1
        , check_oom/2
        , tune_heap_size/1
        , proc_name/2
        , proc_stats/0
        , proc_stats/1
        , rand_seed/0
        , now_to_secs/1
        , now_to_ms/1
        , index_of/2
        , maybe_parse_ip/1
        , ipv6_probe/1
        , ipv6_probe/2
        , pmap/2
        , pmap/3
        ]).

-export([ bin2hexstr_a_f_upper/1
        , bin2hexstr_a_f_lower/1
        , hexstr2bin/1
        ]).

-export([ is_sane_id/1
        , is_sane_id/3
        ]).

-export([
    nolink_apply/1,
    nolink_apply/2
]).

-export([redact/1]).

-define(VALID_STR_RE, "^[A-Za-z0-9]+[A-Za-z0-9-_]*$").
-define(DEFAULT_PMAP_TIMEOUT, 5000).

-spec is_sane_id(list() | binary()) -> ok | {error, Reason::binary()}.
is_sane_id(Str) ->
    is_sane_id(Str, 0, 256).

is_sane_id(Str, Min, Max) ->
    StrLen = len(Str),
    case StrLen > Min andalso StrLen =< Max of
        true ->
            case re:run(Str, ?VALID_STR_RE) of
                nomatch -> {error, <<"required: " ?VALID_STR_RE>>};
                _ -> ok
            end;
        false ->
            Msg = <<(integer_to_binary(Min))/binary,
                <<" < Length =< ">>/binary,
                (integer_to_binary(Max))/binary>>,
            {error, Msg}
    end.

len(Bin) when is_binary(Bin) -> byte_size(Bin);
len(Str) when is_list(Str) -> length(Str).

-define(OOM_FACTOR, 1.25).

%% @doc Parse v4 or v6 string format address to tuple.
%% `Host' itself is returned if it's not an ip string.
maybe_parse_ip(Host) ->
    case inet:parse_address(Host) of
        {ok, Addr} when is_tuple(Addr) -> Addr;
        {error, einval} -> Host
    end.

%% @doc Add `ipv6_probe' socket option if it's supported.
ipv6_probe(Opts) ->
    ipv6_probe(Opts, true).

ipv6_probe(Opts, Ipv6Probe) when is_boolean(Ipv6Probe) orelse is_integer(Ipv6Probe) ->
    Bool = try gen_tcp:ipv6_probe()
           catch _ : _ -> false end,
    ipv6_probe(Bool, Opts, Ipv6Probe).

ipv6_probe(false, Opts, _) -> Opts;
ipv6_probe(true, Opts, Ipv6Probe) -> [{ipv6_probe, Ipv6Probe} | Opts].

%% @doc Merge options
-spec(merge_opts(Opts, Opts) -> Opts when Opts :: proplists:proplist()).
merge_opts(Defaults, Options) ->
    lists:foldl(
      fun({Opt, Val}, Acc) ->
          lists:keystore(Opt, 1, Acc, {Opt, Val});
         (Opt, Acc) ->
          lists:usort([Opt | Acc])
      end, Defaults, Options).

%% @doc Apply a function to a maybe argument.
-spec(maybe_apply(fun((maybe(A)) -> maybe(A)), maybe(A))
      -> maybe(A) when A :: any()).
maybe_apply(_Fun, undefined) -> undefined;
maybe_apply(Fun, Arg) when is_function(Fun) ->
    erlang:apply(Fun, [Arg]).

-spec(compose(list(F)) -> G
  when F :: fun((any()) -> any()),
       G :: fun((any()) -> any())).
compose([F | More]) -> compose(F, More).

-spec(compose(F, G | [Gs]) -> C
  when F :: fun((X1) -> X2),
       G :: fun((X2) -> X3),
       Gs :: [fun((Xn) -> Xn1)],
       C :: fun((X1) -> Xm),
       X3 :: any(), Xn :: any(), Xn1 :: any(), Xm :: any()).
compose(F, G) when is_function(G) -> fun(X) -> G(F(X)) end;
compose(F, [G]) -> compose(F, G);
compose(F, [G | More]) -> compose(compose(F, G), More).

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
        {ok, NState} ->
            pipeline(More, Input, NState);
        {ok, Output, NState} ->
            pipeline(More, Output, NState);
        {error, Reason} ->
            {error, Reason, State};
        {error, Reason, NState} ->
            {error, Reason, NState}
    end.

-compile({inline, [apply_fun/3]}).
apply_fun(Fun, Input, State) ->
    case erlang:fun_info(Fun, arity) of
        {arity, 1} -> Fun(Input);
        {arity, 2} -> Fun(Input, State)
    end.

-spec(start_timer(integer(), term()) -> reference()).
start_timer(Interval, Msg) ->
    start_timer(Interval, self(), Msg).

-spec(start_timer(integer(), pid() | atom(), term()) -> reference()).
start_timer(Interval, Dest, Msg) ->
    erlang:start_timer(erlang:ceil(Interval), Dest, Msg).

-spec(cancel_timer(maybe(reference())) -> ok).
cancel_timer(Timer) when is_reference(Timer) ->
    case erlang:cancel_timer(Timer) of
        false ->
            receive {timeout, Timer, _} -> ok after 0 -> ok end;
        _ -> ok
    end;
cancel_timer(_) -> ok.

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
            drain_deliver(N-1, [Deliver | Acc])
    after 0 ->
        lists:reverse(Acc)
    end.

%% @doc Drain process 'DOWN' events.
-spec(drain_down(pos_integer()) -> list(pid())).
drain_down(Cnt) when Cnt > 0 ->
    drain_down(Cnt, []).

drain_down(0, Acc) ->
    lists:reverse(Acc);
drain_down(Cnt, Acc) ->
    receive
        {'DOWN', _MRef, process, Pid, _Reason} ->
            drain_down(Cnt-1, [Pid | Acc])
    after 0 ->
        lists:reverse(Acc)
    end.

%% @doc Check process's mailbox and heapsize against OOM policy,
%% return `ok | {shutdown, Reason}' accordingly.
%% `ok': There is nothing out of the ordinary.
%% `shutdown': Some numbers (message queue length hit the limit),
%%             hence shutdown for greater good (system stability).
-spec(check_oom(emqx_types:oom_policy()) -> ok | {shutdown, term()}).
check_oom(Policy) ->
    check_oom(self(), Policy).

-spec(check_oom(pid(), emqx_types:oom_policy()) -> ok | {shutdown, term()}).
check_oom(Pid, #{message_queue_len := MaxQLen,
                 max_heap_size := MaxHeapSize}) ->
    case process_info(Pid, [message_queue_len, total_heap_size]) of
        undefined -> ok;
        [{message_queue_len, QLen}, {total_heap_size, HeapSize}] ->
            do_check_oom([{QLen, MaxQLen, message_queue_too_long},
                          {HeapSize, MaxHeapSize, proc_heap_too_large}
                         ])
    end.

do_check_oom([]) -> ok;
do_check_oom([{Val, Max, Reason} | Rest]) ->
    case is_integer(Max) andalso (0 < Max) andalso (Max < Val) of
        true  -> {shutdown, Reason};
        false -> do_check_oom(Rest)
    end.

tune_heap_size(#{max_heap_size := MaxHeapSize}) ->
    %% If set to zero, the limit is disabled.
    erlang:process_flag(max_heap_size, #{size => must_kill_heap_size(MaxHeapSize),
                                         kill => true,
                                         error_logger => true
                                        });
tune_heap_size(undefined) -> ok.

must_kill_heap_size(Size) ->
    %% We set the max allowed heap size by `erlang:process_flag(max_heap_size, #{size => Size})`,
    %% where the `Size` cannot be set to an integer lager than `(1 bsl 59) - 1` on a 64-bit system,
    %% or `(1 bsl 27) - 1` on a 32-bit system.
    MaxAllowedSize = case erlang:system_info(wordsize) of
        8 -> % arch_64
            (1 bsl 59) - 1;
        4 -> % arch_32
            (1 bsl 27) - 1
    end,
    %% We multiply the size with factor ?OOM_FACTOR, to give the
    %% process a chance to suicide by `check_oom/1`
    case ceil(Size * ?OOM_FACTOR) of
        Size0 when Size0 >= MaxAllowedSize -> MaxAllowedSize;
        Size0 -> Size0
    end.

-spec(proc_name(atom(), pos_integer()) -> atom()).
proc_name(Mod, Id) ->
    list_to_atom(lists:concat([Mod, "_", Id])).

%% Get Proc's Stats.
-spec(proc_stats() -> emqx_types:stats()).
proc_stats() -> proc_stats(self()).

-spec(proc_stats(pid()) -> emqx_types:stats()).
proc_stats(Pid) ->
    case process_info(Pid, [message_queue_len,
                            heap_size,
                            total_heap_size,
                            reductions,
                            memory]) of
        undefined -> [];
        [{message_queue_len, Len} | ProcStats] ->
            [{mailbox_len, Len} | ProcStats]
    end.

rand_seed() ->
    rand:seed(exsplus, erlang:timestamp()).

-spec(now_to_secs(erlang:timestamp()) -> pos_integer()).
now_to_secs({MegaSecs, Secs, _MicroSecs}) ->
    MegaSecs * 1000000 + Secs.

-spec(now_to_ms(erlang:timestamp()) -> pos_integer()).
now_to_ms({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000 + round(MicroSecs/1000).

%% lists:index_of/2
index_of(E, L) ->
    index_of(E, 1, L).

index_of(_E, _I, []) ->
    error(badarg);
index_of(E, I, [E | _]) ->
    I;
index_of(E, I, [_ | L]) ->
    index_of(E, I+1, L).

-spec(bin2hexstr_a_f_upper(binary()) -> binary()).
bin2hexstr_a_f_upper(B) when is_binary(B) ->
    << <<(int2hexchar(H, upper)), (int2hexchar(L, upper))>> || <<H:4, L:4>> <= B>>.

-spec(bin2hexstr_a_f_lower(binary()) -> binary()).
bin2hexstr_a_f_lower(B) when is_binary(B) ->
    << <<(int2hexchar(H, lower)), (int2hexchar(L, lower))>> || <<H:4, L:4>> <= B>>.

int2hexchar(I, _) when I >= 0 andalso I < 10 -> I + $0;
int2hexchar(I, upper) -> I - 10 + $A;
int2hexchar(I, lower) -> I - 10 + $a.

-spec(hexstr2bin(binary()) -> binary()).
hexstr2bin(B) when is_binary(B) ->
    hexstr2bin(B, erlang:bit_size(B)).

hexstr2bin(B, Size) when is_binary(B) ->
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
    Middleman = erlang:spawn(make_middleman_fn(Caller, Fun, ResRef)),
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

-spec make_middleman_fn(pid(), fun(() -> any()), reference()) -> fun(() -> no_return()).
make_middleman_fn(Caller, Fun, ResRef) ->
    fun() ->
        process_flag(trap_exit, true),
        CallerMRef = erlang:monitor(process, Caller),
        Worker = erlang:spawn_link(make_worker_fn(Caller, Fun, ResRef)),
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
    end.

-spec make_worker_fn(pid(), fun(() -> any()), reference()) -> fun(() -> no_return()).
make_worker_fn(Caller, Fun, ResRef) ->
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
    end.

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

%% @doc Call this function to avoid logs printed to RPC caller node.
-spec maybe_mute_rpc_log() -> ok.
maybe_mute_rpc_log() ->
    GlNode = node(group_leader()),
    maybe_mute_rpc_log(GlNode).

maybe_mute_rpc_log(Node) when Node =:= node() ->
    %% do nothing, this is a local call
    ok;
maybe_mute_rpc_log(Node) ->
    case atom_to_list(Node) of
        "remsh" ++ _ ->
            %% this is either an upgrade script or nodetool
            %% do nothing, the log may go to the 'emqx' command line console
            ok;
        _ ->
            %% otherwise set group leader to local node
            _ = group_leader(whereis(init), self()),
            ok
    end.

is_group_leader_node_connected() ->
    GLNode = node(group_leader()),
    case atom_to_list(GLNode) of
        "remsh" ++ _ ->
            {ok, Nodes} = net_kernel:nodes_info(),
            lists:keymember(GLNode, 1, Nodes);
        _ ->
            %% for a non-remsh node
            %% as long as the group leader is showing up
            %% it should be connected
            true
    end.

%% @doc Return 'standard_io' or 'standard_error' io device depending on the
%% current group leader node.
%% EMQX nodes do not use EPMD, there is no way for a EMQX node to connect to
%% a remsh node which is not directly connected already.
%% In this case, io:format may fail with {io,terminated} exception.
%% For io:format calls which can be evaluated from RPC calls, call this
%% function to get a safe io device.
safe_io_device() ->
    case is_group_leader_node_connected() of
        true ->
            standard_io;
        false ->
            standard_error
    end.

is_sensitive_key(ws_cookie) -> true;
is_sensitive_key("ws_cookie") -> true;
is_sensitive_key(<<"ws_cookie">>) -> true;
is_sensitive_key(token) -> true;
is_sensitive_key("token") -> true;
is_sensitive_key(<<"token">>) -> true;
is_sensitive_key(password) -> true;
is_sensitive_key("password") -> true;
is_sensitive_key(<<"password">>) -> true;
is_sensitive_key(secret) -> true;
is_sensitive_key("secret") -> true;
is_sensitive_key(<<"secret">>) -> true;
is_sensitive_key(passcode) -> true;
is_sensitive_key("passcode") -> true;
is_sensitive_key(<<"passcode">>) -> true;
is_sensitive_key(passphrase) -> true;
is_sensitive_key("passphrase") -> true;
is_sensitive_key(<<"passphrase">>) -> true;
is_sensitive_key(key) -> true;
is_sensitive_key("key") -> true;
is_sensitive_key(<<"key">>) -> true;
is_sensitive_key(aws_secret_access_key) -> true;
is_sensitive_key("aws_secret_access_key") -> true;
is_sensitive_key(<<"aws_secret_access_key">>) -> true;
is_sensitive_key(secret_key) -> true;
is_sensitive_key("secret_key") -> true;
is_sensitive_key(<<"secret_key">>) -> true;
is_sensitive_key(bind_password) -> true;
is_sensitive_key("bind_password") -> true;
is_sensitive_key(<<"bind_password">>) -> true;
is_sensitive_key(_) -> false.

redact(L) when is_list(L) ->
    lists:map(fun redact/1, L);
redact(M) when is_map(M) ->
    maps:map(fun(K, V) ->
                     redact(K, V)
             end, M);
redact({Key, Value}) ->
    case is_sensitive_key(Key) of
        true ->
            {Key, redact_v(Value)};
        false ->
            {redact(Key), redact(Value)}
    end;
redact(T) when is_tuple(T) ->
    Elements = erlang:tuple_to_list(T),
    Redact = redact(Elements),
    erlang:list_to_tuple(Redact);
redact(Any) ->
    Any.

redact(K, V) ->
    case is_sensitive_key(K) of
        true ->
            redact_v(V);
        false ->
            redact(V)
    end.

-define(REDACT_VAL, "******").
redact_v(V) when is_binary(V) -> <<?REDACT_VAL>>;
redact_v(_V) -> ?REDACT_VAL.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

ipv6_probe_test() ->
    ?assertEqual([{ipv6_probe, true}], ipv6_probe([])).

is_sane_id_test() ->
    ?assertMatch({error, _}, is_sane_id("")),
    ?assertMatch({error, _}, is_sane_id("_")),
    ?assertMatch({error, _}, is_sane_id("_aaa")),
    ?assertMatch({error, _}, is_sane_id("lkad/oddl")),
    ?assertMatch({error, _}, is_sane_id("lkad*oddl")),
    ?assertMatch({error, _}, is_sane_id("script>lkadoddl")),
    ?assertMatch({error, _}, is_sane_id("<script>lkadoddl")),

    ?assertMatch(ok, is_sane_id(<<"Abckdf_lkdfd_1222">>)),
    ?assertMatch(ok, is_sane_id("Abckdf_lkdfd_1222")),
    ?assertMatch(ok, is_sane_id("abckdf_lkdfd_1222")),
    ?assertMatch(ok, is_sane_id("abckdflkdfd1222")),
    ?assertMatch(ok, is_sane_id("abckdflkdf")),
    ?assertMatch(ok, is_sane_id("a1122222")),
    ?assertMatch(ok, is_sane_id("1223333434")),
    ?assertMatch(ok, is_sane_id("1lkdfaldk")),

    Ok = lists:flatten(lists:duplicate(256, "a")),
    Bad = Ok ++ "a",
    ?assertMatch(ok, is_sane_id(Ok)),
    ?assertMatch(ok, is_sane_id(list_to_binary(Ok))),
    ?assertMatch({error, _}, is_sane_id(Bad)),
    ?assertMatch({error, _}, is_sane_id(list_to_binary(Bad))),
    ok.

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
        secret,
        passcode,
        passphrase,
        key,
        aws_secret_access_key,
        secret_key,
        bind_password
    ],
    [{case_name(Type, Key), fun() -> Case(Type, Key) end} || Key <- Keys, Type <- Types].

case_name(Type, Key) ->
    lists:concat([Type, "-", Key]).

-endif.
