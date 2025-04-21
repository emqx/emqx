-module(emqx_mcp_message).
-include("emqx_mcp_gateway.hrl").

-export([
    json_rpc_request/3,
    json_rpc_response/2,
    json_rpc_error/4,
    json_rpc_notification/1, json_rpc_notification/2
]).

-export([
    initialize_request/2, initialize_request/3, initialize_response/3, initialized_notification/0
]).

-export([decode_rpc_msg/1]).

%%==============================================================================
%% MCP Requests/Responses/Notifications
%%==============================================================================
initialize_request(ClientInfo, Capabilities) ->
    initialize_request(1, ClientInfo, Capabilities).

initialize_request(Id, ClientInfo, Capabilities) ->
    json_rpc_request(
        Id,
        <<"initialize">>,
        #{
            <<"protocolVersion">> => ?MCP_VERSION,
            <<"clientInfo">> => ClientInfo,
            <<"capabilities">> => Capabilities
        }
    ).

initialize_response(Id, ServerInfo, Capabilities) ->
    json_rpc_response(
        Id,
        #{
            <<"protocolVersion">> => ?MCP_VERSION,
            <<"serverInfo">> => ServerInfo,
            <<"capabilities">> => Capabilities
        }
    ).

initialized_notification() ->
    json_rpc_notification(<<"notifications/initialized">>).

%%==============================================================================
%% JSON RPC Messages
%%==============================================================================
json_rpc_request(Id, Method, Params) ->
    emqx_utils_json:encode(#{
        <<"jsonrpc">> => <<"2.0">>,
        <<"method">> => Method,
        <<"params">> => Params,
        <<"id">> => Id
    }).

json_rpc_response(Id, Result) ->
    emqx_utils_json:encode(#{
        <<"jsonrpc">> => <<"2.0">>,
        <<"result">> => Result,
        <<"id">> => Id
    }).

json_rpc_notification(Method) ->
    emqx_utils_json:encode(#{
        <<"jsonrpc">> => <<"2.0">>,
        <<"method">> => Method
    }).

json_rpc_notification(Method, Params) ->
    emqx_utils_json:encode(#{
        <<"jsonrpc">> => <<"2.0">>,
        <<"method">> => Method,
        <<"params">> => Params
    }).

json_rpc_error(Id, Code, Message, Data) ->
    emqx_utils_json:encode(#{
        <<"jsonrpc">> => <<"2.0">>,
        <<"error">> => #{
            <<"code">> => Code,
            <<"message">> => Message,
            <<"data">> => Data
        },
        <<"id">> => Id
    }).

decode_rpc_msg(Msg) ->
    try emqx_utils_json:decode(Msg) of
        #{
            <<"jsonrpc">> := <<"2.0">>,
            <<"method">> := Method,
            <<"params">> := Params,
            <<"id">> := Id
        } ->
            {ok, #{type => json_rpc_request, method => Method, id => Id, params => Params}};
        #{<<"jsonrpc">> := <<"2.0">>, <<"result">> := Result, <<"id">> := Id} ->
            {ok, #{type => json_rpc_response, id => Id, result => Result}};
        #{<<"jsonrpc">> := <<"2.0">>, <<"error">> := Error, <<"id">> := Id} ->
            {ok, #{type => json_rpc_error, id => Id, error => Error}};
        #{<<"jsonrpc">> := <<"2.0">>, <<"method">> := Method, <<"params">> := Params} ->
            {ok, #{type => json_rpc_notification, method => Method, params => Params}};
        Msg ->
            {error, {invalid_json_rpc_msg, Msg}}
    catch
        error:Reason ->
            {error, {invalid_json, Msg, Reason}}
    end.
