%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_connector_lib).

%% connectivity check
-export([
    http_connectivity/2,
    tcp_connectivity/3
]).

-export([resolve_dns/2]).

-spec http_connectivity(uri_string:uri_string(), timeout()) ->
    ok | {error, Reason :: term()}.
http_connectivity(Url, Timeout) ->
    case emqx_http_lib:uri_parse(Url) of
        {ok, #{host := Host, port := Port}} ->
            tcp_connectivity(Host, Port, Timeout);
        {error, Reason} ->
            {error, Reason}
    end.

-spec tcp_connectivity(
    Host :: inet:socket_address() | inet:hostname(),
    Port :: inet:port_number(),
    timeout()
) ->
    ok | {error, Reason :: term()}.
tcp_connectivity(Host, Port, Timeout) ->
    case gen_tcp:connect(Host, Port, emqx_utils:ipv6_probe([]), Timeout) of
        {ok, Sock} ->
            gen_tcp:close(Sock),
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Mostly for meck.
resolve_dns(DNS, Type) ->
    inet_res:lookup(DNS, in, Type).
