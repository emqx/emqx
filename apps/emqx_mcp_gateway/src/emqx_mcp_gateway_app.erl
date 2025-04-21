%%%-------------------------------------------------------------------
%% @doc emqx_mcp_gateway public API
%% @end
%%%-------------------------------------------------------------------

-module(emqx_mcp_gateway_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    Ret = emqx_mcp_gateway_sup:start_link(),
    case emqx_conf:get([mcp], #{enable => false}) of
        #{enable := true} -> emqx_mcp_gateway:enable();
        _ -> ok
    end,
    Ret.

stop(_State) ->
    emqx_mcp_gateway:disable(),
    ok.

%% internal functions
