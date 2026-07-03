%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connection_util).
-moduledoc "Common utilities for connection module implementations".

-export([
    label_process/2
]).

%%

-type listener() :: {emqx_listeners:listener_type(), _Name :: atom()}.

-spec label_process(listener(), emqx_types:conninfo()) -> ok.
label_process(
    {Type, ListenerName},
    #{
        peername := Peername,
        conn_mod := ConnMod
    } = _ConnInfo
) ->
    PeernameStr = format_peername(Peername),
    ListenerId = emqx_listeners:listener_id(Type, ListenerName),
    proc_lib:set_label({ListenerId, PeernameStr}),
    emqx_logger:set_proc_metadata(#{
        listener => ListenerId,
        peername => PeernameStr,
        connmod => ConnMod
    }).

-spec format_peername({inet:ip_address(), inet:port_number()}) -> binary().
format_peername({Addr, Port}) ->
    iolist_to_binary([inet:ntoa(Addr), $:, integer_to_list(Port)]).
