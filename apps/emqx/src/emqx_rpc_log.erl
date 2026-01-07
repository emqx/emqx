%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Log callback for gen_rpc 3.5 or newer.
%% Throttle the log message failed_to_connect_server.
-module(emqx_rpc_log).
-export([init/0, log/4]).

-include("logger.hrl").

-define(META(TYPE), #{tag => "RPC", domain => [gen_rpc, TYPE]}).

%% Called during boot sequence to set gen_rpc logger callback.
init() ->
    gen_rpc:set_logger(?MODULE).

%% Called by gen_rpc_logger.
log(Level, Type, Msg, Data) ->
    case Msg of
        "failed_to_connect_server" ->
            ?SLOG_THROTTLE(Level, Data#{msg => failed_to_connect_server}, ?META(Type));
        _ ->
            ?SLOG(Level, Data#{msg => Msg}, ?META(Type))
    end.
