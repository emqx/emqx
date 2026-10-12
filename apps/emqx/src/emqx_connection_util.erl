%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connection_util).
-moduledoc "Common utilities for connection module implementations".

-export([
    label_process/2,
    sock_error_shutdown/1
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
