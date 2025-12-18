%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_tls13_client).

-moduledoc """
TLS 1.3 Client Test Module for Session Resumption

This module implements a gen_statem-based TLS 1.3 client that tests
session ticket resumption functionality. It connects to a TLS server,
receives session tickets, disconnects, and then reconnects using the
received ticket to verify that session resumption works correctly.

## Usage

There are two ways to use this module:

### Using Certificate Directory

```erlang
emqx_tls13_client:run("localhost", 8883, "/path/to/certs")
```

### Using Base SSL Options

```erlang
SslOpts = [
    {verify, verify_peer},
    {versions, ['tlsv1.3']},
    {session_tickets, manual},
    {active, true},
    {keyfile, "/path/to/client.key"},
    {certfile, "/path/to/client.pem"},
    {cacertfile, "/path/to/ca.pem"},
    {customize_hostname_check, [
        {match_fun, fun(_CertDomain, _UserDomain) -> true end}
    ]}
],
emqx_tls13_client:run2("localhost", 8883, SslOpts)
```

Both functions will:
1. Start the SSL application
2. Connect to the server at the specified host and port
3. Wait for a session ticket from the server
4. Disconnect and reconnect using the session ticket
5. Verify that the session was resumed (protocol is TLS 1.3 and
   session_resumption is true)
6. Return `ok` on success or `{error, Reason}` on failure

## Certificate Requirements

The CertDir parameter should contain the following files:
- `client-key.pem` - Client private key
- `client-cert.pem` - Client certificate
- `cacert.pem` - CA certificate for server verification

## SSL Options

The client uses the following SSL options:
- `verify_peer` - Verifies the server certificate
- `versions: ['tlsv1.3']` - Only TLS 1.3
- `session_tickets: manual` - Manual session ticket handling
- `active: true` - Receives SSL messages asynchronously
- `customize_hostname_check` - Custom hostname verification that
  always returns true
""".

-behaviour(gen_statem).

-export([
    start_link/3,
    run/3,
    run2/3
]).

-export([
    init/1,
    callback_mode/0,
    terminate/3
]).

%% States
-export([
    connecting/3,
    connected/3,
    reconnecting/3,
    resumed/3
]).

-record(state, {
    host :: inet:ip_address() | inet:hostname(),
    port :: inet:port_number(),
    socket :: ssl:sslsocket() | undefined,
    session_ticket :: map() | undefined,
    ssl_opts :: [ssl:ssl_option()],
    ticket_received :: boolean()
}).

%% API

start_link(Host, Port, SslOpts) ->
    gen_statem:start_link(?MODULE, {Host, Port, SslOpts}, []).

run(Host, Port, CertDir) ->
    %% Prepare SSL options from certificate directory
    KeyFile = filename:join(CertDir, "client-key.pem"),
    CertFile = filename:join(CertDir, "client-cert.pem"),
    CaCertFile = filename:join(CertDir, "cacert.pem"),
    SslOpts = [
        {verify, verify_peer},
        {versions, ['tlsv1.3']},
        {session_tickets, manual},
        {active, true},
        {keyfile, KeyFile},
        {certfile, CertFile},
        {cacertfile, CaCertFile},
        {customize_hostname_check, [
            {match_fun, fun(_CertDomain, _UserDomain) -> true end}
        ]}
    ],
    run2(Host, Port, SslOpts).

run2(Host, Port, BaseSslOpts) ->
    %% Ensure SSL application and its dependencies are started
    {ok, _} = application:ensure_all_started(ssl),
    process_flag(trap_exit, true),
    {ok, Pid} = start_link(Host, Port, BaseSslOpts),
    receive
        {'EXIT', Pid, ExitReason} ->
            case ExitReason of
                normal -> ok;
                _ -> {error, ExitReason}
            end
    end.

%% gen_statem callbacks

init({Host, Port, BaseSslOpts}) ->
    %% BaseSslOpts should already include all necessary options
    %% Add required options if not present
    SslOpts = ensure_required_opts(BaseSslOpts),
    State = #state{
        host = Host,
        port = Port,
        ssl_opts = SslOpts,
        ticket_received = false
    },
    {ok, connecting, State, [{next_event, internal, connect}]}.

ensure_required_opts(Opts) ->
    %% Ensure required options are present
    Opts1 =
        case proplists:is_defined(versions, Opts) of
            false -> [{versions, ['tlsv1.3']} | Opts];
            true -> Opts
        end,
    Opts2 =
        case proplists:is_defined(session_tickets, Opts1) of
            false -> [{session_tickets, manual} | Opts1];
            true -> Opts1
        end,
    Opts3 =
        case proplists:is_defined(active, Opts2) of
            false -> [{active, true} | Opts2];
            true -> Opts2
        end,
    Opts3.

callback_mode() ->
    state_functions.

connecting(internal, connect, #state{host = Host, port = Port, ssl_opts = Opts} = State) ->
    case ssl:connect(Host, Port, Opts, 5000) of
        {ok, Socket} ->
            {next_state, connected, State#state{socket = Socket}, [
                {state_timeout, 2000, check_ticket}
            ]};
        {error, Reason} ->
            {stop, {connect_failed, Reason}}
    end.

connected(info, {ssl, session_ticket, Ticket}, #state{} = State) when is_map(Ticket) ->
    %% Session ticket received from server
    {keep_state, State#state{session_ticket = Ticket, ticket_received = true}};
connected(
    state_timeout,
    check_ticket,
    #state{socket = Socket, ticket_received = Received, session_ticket = Ticket} = State
) ->
    case {Received, Ticket} of
        {true, Ticket} when is_map(Ticket) ->
            %% Ticket received, close connection and reconnect
            ssl:close(Socket),
            {next_state, reconnecting, State#state{socket = undefined}, [
                {next_event, internal, reconnect}
            ]};
        {false, _} ->
            %% No ticket received yet, wait a bit more
            {keep_state, State, [{state_timeout, 1000, check_ticket}]};
        _ ->
            {stop, no_session_ticket_received}
    end;
connected(info, {ssl_closed, Socket}, #state{socket = Socket} = State) ->
    %% Connection closed by server
    {keep_state, State#state{socket = undefined}};
connected(info, {ssl_error, Socket, _Reason}, #state{socket = Socket} = State) ->
    %% SSL error occurred
    {keep_state, State#state{socket = undefined}};
connected(_EventType, _Event, State) ->
    %% Ignore other events
    {keep_state, State}.

reconnecting(
    internal,
    reconnect,
    #state{host = Host, port = Port, session_ticket = Ticket, ssl_opts = BaseOpts} = State
) ->
    case Ticket of
        undefined ->
            {stop, no_session_ticket_available};
        Ticket when is_map(Ticket) ->
            %% Use the ticket for resumption - use_ticket expects a list
            ReconnectOpts = [{use_ticket, [Ticket]} | BaseOpts],
            case ssl:connect(Host, Port, ReconnectOpts, 5000) of
                {ok, Socket} ->
                    {next_state, resumed, State#state{socket = Socket}, [
                        {next_event, internal, verify_resumption}
                    ]};
                {error, Reason} ->
                    {stop, {reconnect_failed, Reason}}
            end
    end.

resumed(internal, verify_resumption, #state{socket = Socket}) ->
    case ssl:connection_information(Socket, [protocol, session_resumption]) of
        {ok, Info} ->
            Protocol = proplists:get_value(protocol, Info),
            SessionResumption = proplists:get_value(session_resumption, Info),
            case {Protocol, SessionResumption} of
                {'tlsv1.3', true} ->
                    ssl:close(Socket),
                    {stop, normal};
                {Protocol, false} ->
                    {stop, {session_resumption_failed, Protocol, SessionResumption}};
                {Protocol, SessionResumption} ->
                    {stop, {unexpected_protocol_or_resumption, Protocol, SessionResumption}}
            end;
        {error, Reason} ->
            {stop, {connection_info_failed, Reason}}
    end;
resumed(info, {ssl_closed, Socket}, #state{socket = Socket} = State) ->
    %% Connection closed
    {keep_state, State#state{socket = undefined}};
resumed(_EventType, _Event, State) ->
    %% Ignore other events
    {keep_state, State}.

terminate(_Reason, _StateName, #state{socket = Socket}) ->
    case Socket of
        undefined -> ok;
        _ -> ssl:close(Socket)
    end,
    ok.
