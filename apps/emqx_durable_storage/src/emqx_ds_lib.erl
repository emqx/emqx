%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_lib).

-include("emqx_ds.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%% API:
-export([with_worker/3, terminate/3, send_after/3, cancel_timer/2]).

%% internal exports:
-export([]).

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
    erlang:cancel_timer(TRef),
    receive
        TimeoutMsg ->
            ok
    after 0 ->
        ok
    end.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================
