%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_lib).

-include("emqx_ds.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%% API:
-export([
    with_worker/3,
    autoclean/4,
    terminate/3,
    send_after/3,
    cancel_timer/2,
    ets_delete/1,
    tf_to_asn1/1,
    asn1_to_tf/1,
    resolve_db_group/1
]).

%% internal exports:
-export([autoclean_entrypoint/3]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

%%================================================================================
%% API functions
%%================================================================================

%% @doc The caller will receive message of type `{reference(), Result | {error, unrecoverable, map()}'
-spec with_worker(module(), atom(), list()) -> {ok, pid(), reference()}.
with_worker(Mod, Function, Args) ->
    ReplyTo = alias([reply]),
    Pid = spawn_opt(
        fun() ->
            Result =
                try
                    apply(Mod, Function, Args)
                catch
                    EC:Err:Stack ->
                        {error, unrecoverable, #{
                            msg => ?FUNCTION_NAME,
                            EC => Err,
                            stacktrace => Stack
                        }}
                end,
            ReplyTo ! {ReplyTo, Result}
        end,
        [link, {min_heap_size, 10000}]
    ),
    {ok, Pid, ReplyTo}.

-doc """
Return supervisor child specification that allows to tie shard
readiness optvar to a supervisor.
""".
-spec autoclean(atom(), timeout(), Setup, Teardown) -> supervisor:child_spec() when
    Setup :: fun(() -> _),
    Teardown :: fun(() -> _).
autoclean(Id, CleanupTimeout, Setup, Teardown) ->
    #{
        id => Id,
        start => {proc_lib, start_link, [?MODULE, autoclean_entrypoint, [self(), Setup, Teardown]]},
        shutdown => CleanupTimeout,
        type => worker,
        restart => permanent
    }.

-spec terminate(module(), _Reason, map()) -> ok.
terminate(Module, Reason, Misc) when Reason =:= shutdown; Reason =:= normal ->
    ?tp(emqx_ds_process_terminate, Misc#{module => Module, reason => Reason});
terminate(Module, Reason, Misc) ->
    ?tp(warning, emqx_ds_abnormal_process_terminate, Misc#{module => Module, reason => Reason}).

-spec send_after(timeout(), pid(), _Message) -> undefined | reference().
send_after(infinity, _, _) ->
    undefined;
send_after(Timeout, Dest, Msg) when is_integer(Timeout) ->
    erlang:send_after(Timeout, Dest, Msg).

-spec cancel_timer(undefined | reference(), _Message) -> ok.
cancel_timer(undefined, _) ->
    ok;
cancel_timer(TRef, TimeoutMsg) ->
    _ = erlang:cancel_timer(TRef),
    receive
        TimeoutMsg ->
            ok
    after 0 ->
        ok
    end.

%% @doc A non-throwing version of `ets:delete/1'
ets_delete(Tid) ->
    try
        ets:delete(Tid)
    catch
        _:_ ->
            ok
    end.

%% @doc Transform normal representation of the topic filter to
%% serializable representation defined in DSMetadataCommon ASN.1
%% schema
tf_to_asn1(TF) ->
    lists:map(
        fun(Level) ->
            case Level of
                '+' ->
                    {plus, 'NULL'};
                '#' ->
                    {hash, 'NULL'};
                B when is_binary(B) ->
                    {const, B};
                '' ->
                    {const, <<>>}
            end
        end,
        TF
    ).

%% @doc Opposite to tf_to_asn1 modulo empty topic level
asn1_to_tf(ASN1) ->
    lists:map(
        fun(Level) ->
            case Level of
                {plus, 'NULL'} ->
                    '+';
                {hash, 'NULL'} ->
                    '#';
                {const, Const} ->
                    Const
            end
        end,
        ASN1
    ).

resolve_db_group(Options = #{db_group := DBGroupName}) ->
    {ok, DBGroup} = emqx_ds:lookup_db_group(DBGroupName),
    Options#{db_group := DBGroup}.

%%================================================================================
%% Internal exports
%%================================================================================

autoclean_entrypoint(Parent, Setup, Teardown) ->
    process_flag(trap_exit, true),
    Setup(),
    proc_lib:init_ack({ok, self()}),
    Loop = fun Loop() ->
        receive
            {'EXIT', Parent, shutdown} ->
                Teardown(),
                exit(shutdown);
            _ ->
                Loop()
        end
    end,
    Loop().

%%================================================================================
%% Internal functions
%%================================================================================
