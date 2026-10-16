%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connection_util).
-moduledoc "Common utilities for connection module implementations".

-export([
    label_process/2,
    sock_error_shutdown/1,
    check_oom/1,
    check_oom/2
]).

%%

-type listener() :: {emqx_listeners:listener_type(), _Name :: atom()}.

-spec label_process(listener(), emqx_types:conninfo()) -> ok.
label_process(
    {Type, ListenerName},
    #{
        peername := Peername
    } = _ConnInfo
) ->
    PeernameStr = format_peername(Peername),
    ListenerId = emqx_listeners:listener_id(Type, ListenerName),
    proc_lib:set_label({ListenerId, PeernameStr}),
    emqx_logger:set_proc_metadata(#{
        listener => ListenerId,
        peername => PeernameStr
    }).

-spec format_peername({inet:ip_address(), inet:port_number()}) -> binary().
format_peername({Addr, Port}) ->
    iolist_to_binary([inet:ntoa(Addr), $:, integer_to_list(Port)]).

-doc """
Return the shutdown reason for a socket error, in a shape that the listener
counts. esockd counts an atom reason, an `{ssl_error, _}` reason and a map with
a `shutdown_count` key, and logs the last two at notice level. It reports any
other reason at error level.

- An atom or an `{ssl_error, _}` reason is returned unchanged.
- A TLS alert counts under its alert kind. `ssl_alert:description_atom/1`
  returns the kind and maps each unknown alert code to
  `'unsupported/unknown_alert'`, so the set of counter names is bounded.
- Any other reason counts as `sock_error`.
""".
-spec sock_error_shutdown(term()) -> atom() | {ssl_error, term()} | map().
sock_error_shutdown(Reason) when is_atom(Reason) ->
    Reason;
sock_error_shutdown({ssl_error, _} = Reason) ->
    Reason;
sock_error_shutdown({tls_alert, {Kind, _Text}} = Reason) when is_atom(Kind) ->
    emqx_channel:shutdown_count(Kind, Reason);
sock_error_shutdown(Reason) ->
    emqx_channel:shutdown_count(sock_error, Reason).

-doc """
Check the mailbox size and the heap size of the calling process against the
force-shutdown policy. See `check_oom/2`.
""".
-spec check_oom(map()) -> ok | {shutdown, map()}.
check_oom(Policy) ->
    check_oom(self(), Policy).

-doc """
Check the mailbox size and the heap size of `Pid` against the force-shutdown
policy.

- Return `ok` when the policy is disabled, when the process is gone, or when
  no limit is exceeded.
- Return `{shutdown, Reason}` when a limit is exceeded. `Reason` is a map with
  the `shutdown_count`, `reason`, `value` and `max` keys, and the `label` key
  when the process has a label. `shutdown_count` and `reason` are
  `mailbox_overflow` or `proc_heap_too_large`, so esockd counts the exit under
  that name.
""".
-spec check_oom(pid(), map()) -> ok | {shutdown, map()}.
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
            Result = do_check_oom([
                {QLen, MaxQLen, mailbox_overflow},
                {HeapSize, MaxHeapSize, proc_heap_too_large}
            ]),
            add_proc_label(Pid, Result)
    end.

do_check_oom([]) ->
    ok;
do_check_oom([{Val, Max, Reason} | Rest]) ->
    case is_integer(Max) andalso (0 < Max) andalso (Max < Val) of
        %% esockd drops `shutdown_count' from its log line, so `reason' keeps
        %% the name in it.
        true ->
            {shutdown,
                emqx_channel:shutdown_count(Reason, #{reason => Reason, value => Val, max => Max})};
        false ->
            do_check_oom(Rest)
    end.

%% Add the process label (e.g. `{clientid, ClientId}') to the shutdown
%% reason so the supervisor report identifies the offending process.
add_proc_label(_Pid, ok) ->
    ok;
add_proc_label(Pid, {shutdown, Reason}) ->
    case proc_lib:get_label(Pid) of
        undefined -> {shutdown, Reason};
        Label -> {shutdown, Reason#{label => Label}}
    end.
