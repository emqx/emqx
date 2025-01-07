%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_s3_client_http).

-include_lib("emqx/include/logger.hrl").

-export([new/2]).
-export([request/6]).

-export([
    connect_timeout/1,
    request_timeout/1
]).

-export([
    start_pool/2,
    update_pool/2,
    stop_pool/1
]).

-export_type([t/0]).

-type t() :: #{
    pool := atom(),
    connect_timeout => _Milliseconds :: pos_integer(),
    request_timeout => _Milliseconds :: pos_integer(),
    transport := tcp | ssl,
    socket => [gen_tcp:connect_option()],
    ssl => [ssl:tls_client_option()],
    %% Hackney options (cached)
    reqopts => list()
}.

-define(DEFAULT_CONNECT_TIMEOUT, 15000).
-define(DEFAULT_REQUEST_TIMEOUT, 30000).

%%

new(PoolName, ProfileConfig) ->
    HTTPOpts = maps:get(transport_options, ProfileConfig, #{}),
    Timeouts = maps:with([connect_timeout, request_timeout], HTTPOpts),
    SocketOpts = #{socket => maybe_ipv6_probe(HTTPOpts)},
    TransportOpts =
        case maps:get(ssl, HTTPOpts, #{}) of
            #{enable := false} ->
                #{transport => tcp};
            #{enable := true} = SSL ->
                SSLOpts = emqx_tls_lib:to_client_opts(SSL),
                #{transport => ssl, ssl => SSLOpts}
        end,
    Client = lists:foldl(
        fun maps:merge/2,
        #{pool => PoolName},
        [Timeouts, SocketOpts, TransportOpts]
    ),
    Client#{
        reqopts => request_options(Client)
    }.

maybe_ipv6_probe(#{ipv6_probe := true}) ->
    emqx_utils:ipv6_probe([]);
maybe_ipv6_probe(#{}) ->
    [].

%%

request(Url0, Method, Headers0, Body, Timeout, #{reqopts := RequestOpts}) ->
    Url = to_binary(Url0),
    Headers = headers_to_hackney(Headers0),
    T0 = erlang:monotonic_time(),
    try hackney:request(Method, Url, Headers, Body, RequestOpts) of
        {ok, Status, RespHeaders} ->
            ?SLOG(debug, #{
                msg => "s3_http_request_ok",
                status => Status,
                headers => RespHeaders,
                time => tdiff(T0)
            }),
            {ok, {{Status, undefined}, headers_to_erlcloud(RespHeaders), undefined}};
        {ok, Status, RespHeaders, Ref} ->
            case hackney:body(Ref) of
                {ok, RespBody} ->
                    ?SLOG(debug, #{
                        msg => "s3_http_request_ok",
                        status => Status,
                        headers => RespHeaders,
                        body => RespBody,
                        time => tdiff(T0)
                    }),
                    {ok, {{Status, undefined}, headers_to_erlcloud(RespHeaders), RespBody}};
                {error, {closed, Partial}} ->
                    ?SLOG(debug, #{
                        msg => "s3_http_request_partial",
                        status => Status,
                        headers => RespHeaders,
                        body => Partial,
                        time => tdiff(T0)
                    }),
                    {ok, {{Status, undefined}, headers_to_erlcloud(RespHeaders), Partial}};
                {error, Reason} ->
                    ?SLOG(warning, #{
                        msg => "s3_http_request_partial_fail",
                        status => Status,
                        headers => RespHeaders,
                        reason => Reason,
                        time => tdiff(T0)
                    }),
                    case Status of
                        S when S >= 200 andalso S < 300 ->
                            {error, {hackney_error, Reason}};
                        _ ->
                            {ok, {{Status, undefined}, headers_to_erlcloud(RespHeaders), undefined}}
                    end
            end;
        {error, Reason} = Error ->
            ?SLOG(warning, #{
                msg => "s3_http_request_fail",
                reason => Reason,
                timeout => Timeout,
                request => Method,
                url => Url0,
                time => tdiff(T0)
            }),
            Error
    catch
        error:Reason ->
            ?SLOG(warning, #{
                msg => "s3_http_request_fail",
                reason => Reason,
                timeout => Timeout,
                method => Method,
                url => Url0
            }),
            {error, Reason}
    end.

tdiff(T0) ->
    T1 = erlang:monotonic_time(),
    erlang:convert_time_unit(T1 - T0, native, millisecond).

request_options(
    Client = #{
        pool := PoolName,
        socket := SocketOpts
    }
) ->
    [
        {pool, PoolName},
        {connect_timeout, connect_timeout(Client)},
        {checkout_timeout, connect_timeout(Client)},
        {recv_timeout, request_timeout(Client)},
        {follow_redirect, false}
    ] ++
        [{connect_options, SocketOpts} || SocketOpts =/= []] ++
        transport_options(Client).

transport_options(#{transport := ssl, ssl := SSLOpts}) ->
    [{ssl_options, SSLOpts}, {insecure, false}];
transport_options(#{transport := tcp}) ->
    [].

%%

connect_timeout(C) ->
    maps:get(connect_timeout, C, ?DEFAULT_CONNECT_TIMEOUT).

request_timeout(C) ->
    maps:get(request_timeout, C, ?DEFAULT_REQUEST_TIMEOUT).

%%

start_pool(PoolName, ProfileConfig) ->
    PoolConfig = pool_config(ProfileConfig),
    ?SLOG(debug, #{msg => "s3_starting_http_pool", pool_name => PoolName, config => PoolConfig}),
    case hackney_pool:start_pool(PoolName, PoolConfig) of
        ok ->
            ?SLOG(info, #{msg => "s3_start_http_pool_success", pool_name => PoolName}),
            ok;
        {error, _} = Error ->
            ?SLOG(error, #{msg => "s3_start_http_pool_fail", pool_name => PoolName, error => Error}),
            Error
    end.

update_pool(PoolName, ProfileConfig) ->
    PoolConfig = pool_config(ProfileConfig),
    ?SLOG(debug, #{msg => "s3_updating_http_pool", pool_name => PoolName, config => PoolConfig}),
    lists:foldl(
        fun
            (PoolSetting, ok) -> update_pool_setting(PoolName, PoolSetting);
            (_, Error) -> Error
        end,
        ok,
        PoolConfig
    ).

update_pool_setting(PoolName, {max_connections, PoolSize}) ->
    hackney_pool:set_max_connections(PoolName, PoolSize);
update_pool_setting(PoolName, {timeout, Keepalive}) ->
    hackney_pool:set_timeout(PoolName, Keepalive).

stop_pool(PoolName) ->
    case hackney_pool:stop_pool(PoolName) of
        ok ->
            ok;
        {error, Reason} ->
            ?SLOG(error, #{msg => "s3_stop_http_pool_fail", pool_name => PoolName, reason => Reason}),
            ok
    end.

pool_config(#{
    transport_options := #{
        pool_type := _PoolType,
        pool_size := PoolSize
    }
}) ->
    [
        {max_connections, PoolSize},
        {timeout, _Keepalive = 60000}
    ].

%% We need some header conversions to tie the emqx_s3, erlcloud and Hackney APIs together.
%% The request header flow is:
%%   UserHeaders --(emqx_s3_client)-> ErlcloudRequestHeaders
%%   ErlcloudRequestHeaders --(erlcloud_httpc)--(emqx_s3_client_http)-> HackneyHeaders
%%   HackneyHeaders --(hackney)-> HTTP Request
%%
%% The response header flow is:
%%   HTTP Response --(hackney)-> HackneyHeaders
%%   HackneyHeaders --(emqx_s3_client_http)--(erlcloud_httpc)-> ErlcloudResponseHeders
%%   ErlcloudResponseHeders --(emqx_s3_client)-> ...
%%
%% UserHeders (emqx_s3 API headers) are maps with string/binary keys.
%% ErlcloudRequestHeaders are lists of tuples with string keys and iodata values
%% ErlcloudResponseHeders are lists of tuples with lower case string keys and iodata values.
%% HackneyHeaders are lists of tuples with binary keys and iodata values.

%% Hackney expects a list of tuples with binary keys and values.
headers_to_hackney(ErlcloudHeaders) ->
    [{to_binary(K), to_binary(V)} || {K, V} <- ErlcloudHeaders].

%% Hackney returns response headers as a list of tuples with binary keys.
%% Erlcloud expects a list of tuples with string values and lowcase string keys
%% from the underlying http library.
headers_to_erlcloud(RespHeaders) ->
    [{string:to_lower(to_list_string(K)), to_list_string(V)} || {K, V} <- RespHeaders].

to_binary(Val) when is_list(Val) ->
    list_to_binary(Val);
to_binary(Val) when is_binary(Val) ->
    Val.

to_list_string(Val) when is_binary(Val) ->
    binary_to_list(Val);
to_list_string(Val) when is_atom(Val) ->
    atom_to_list(Val);
to_list_string(Val) when is_list(Val) ->
    Val.
