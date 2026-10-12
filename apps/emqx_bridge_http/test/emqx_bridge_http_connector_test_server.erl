%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_http_connector_test_server).

-compile([nowarn_export_all, export_all]).

-behaviour(supervisor).
-behaviour(cowboy_handler).
-behaviour(ranch_protocol).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link(Port, Path) ->
    start_link(Port, Path, false).

start_link(Ref, Transport, #{mode := close_after_request, notify := TestPid}) ->
    Pid = proc_lib:spawn_link(fun() ->
        {ok, Socket} = ranch:handshake(Ref),
        ok = Transport:setopts(Socket, [{packet, http_bin}]),
        case read_request(Transport, Socket, 0) of
            {ok, Body} ->
                TestPid ! {http_attempt, Body};
            {error, _} ->
                %% Health checks can open idle connections without sending a request.
                ok
        end,
        %% Close after reading the request, without sending an HTTP response.
        Transport:close(Socket)
    end),
    {ok, Pid};
start_link(Port, Path, SSLOpts) ->
    start_link(Port, Path, SSLOpts, #{}).

start_link(Port, Path, SSLOpts, Opts) ->
    DNS = setup_dns(Opts),
    case Port of
        random ->
            PickedPort = pick_port_number(56000),
            {ok, Pid} = supervisor:start_link({local, ?MODULE}, ?MODULE, [
                PickedPort, Path, SSLOpts, Opts, DNS
            ]),
            {ok, {PickedPort, Pid}};
        _ ->
            supervisor:start_link({local, ?MODULE}, ?MODULE, [Port, Path, SSLOpts, Opts, DNS])
    end.

stop() ->
    restore_dns(),
    try
        gen_server:stop(?MODULE)
    catch
        exit:noproc ->
            ok
    end.

set_handler(F) when is_function(F, 2) ->
    true = ets:insert(?MODULE, {handler, F}),
    ok.

set_dns_available(Available) ->
    [{dns, #{address := Address, host := Host}}] = ets:lookup(?MODULE, dns),
    case Available of
        true ->
            inet_db:add_host(Address, [Host]);
        false ->
            ok = inet_db:del_host(Address),
            %% Existing TCP connections do not need to resolve the host again.
            close_connections()
    end.

%%------------------------------------------------------------------------------
%% supervisor API
%%------------------------------------------------------------------------------

init([Port, Path, SSLOpts, Opts, DNS]) ->
    Tab = ets:new(?MODULE, [set, named_table, public]),
    ets:insert(Tab, [{handler, fun default_handler/2}, {dns, DNS}]),

    {Transport, TransOpts, CowboyModule} = transport_settings(Port, SSLOpts),
    {Protocol, ProtoOpts, ConnectionType} = protocol_settings(Path, CowboyModule, Opts),
    ChildSpec = ranch:child_spec(
        ?MODULE, Transport, TransOpts#{connection_type => ConnectionType}, Protocol, ProtoOpts
    ),

    {ok, {#{}, [ChildSpec]}}.

%%------------------------------------------------------------------------------
%% cowboy_server API
%%------------------------------------------------------------------------------

init(Req, State) ->
    [{handler, Handler}] = ets:lookup(?MODULE, handler),
    Handler(Req, State).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

protocol_settings(_Path, _CowboyModule, #{mode := close_after_request, notify := TestPid}) ->
    {?MODULE, #{mode => close_after_request, notify => TestPid}, worker};
protocol_settings(Path, CowboyModule, _Opts) ->
    Dispatch = cowboy_router:compile([{'_', [{Path, ?MODULE, []}]}]),
    {CowboyModule, #{env => #{dispatch => Dispatch}}, supervisor}.

read_request(Transport, Socket, Length) ->
    case Transport:recv(Socket, 0, 5_000) of
        {ok, {http_request, _, _, _}} ->
            read_request(Transport, Socket, Length);
        {ok, {http_header, _, 'Content-Length', _, Value}} ->
            read_request(Transport, Socket, binary_to_integer(Value));
        {ok, {http_header, _, _, _, _}} ->
            read_request(Transport, Socket, Length);
        {ok, http_eoh} when Length =:= 0 ->
            {ok, <<>>};
        {ok, http_eoh} ->
            ok = Transport:setopts(Socket, [{packet, raw}]),
            Transport:recv(Socket, Length, 5_000);
        {error, _} = Error ->
            Error
    end.

setup_dns(#{dns_control := true, host := Host}) ->
    Address = {127, 0, 0, 2},
    Lookup = inet_db:res_option(lookup),
    DNS = #{address => Address, host => Host, lookup => Lookup},
    emqx_common_test_helpers:on_exit(fun() -> restore_dns(DNS) end),
    ok = inet_db:set_lookup([file]),
    ok = inet_db:add_host(Address, [Host]),
    DNS;
setup_dns(_Opts) ->
    undefined.

restore_dns() ->
    try ets:lookup(?MODULE, dns) of
        [{dns, DNS}] -> restore_dns(DNS)
    catch
        error:badarg -> ok
    end.

restore_dns(undefined) ->
    ok;
restore_dns(#{address := Address, lookup := Lookup}) ->
    ok = inet_db:del_host(Address),
    inet_db:set_lookup(Lookup).

close_connections() ->
    lists:foreach(
        fun(Pid) ->
            Ref = monitor(process, Pid),
            exit(Pid, kill),
            receive
                {'DOWN', Ref, process, Pid, _} -> ok
            end
        end,
        ranch:procs(?MODULE, connections)
    ).

transport_settings(Port, _SSLOpts = false) ->
    TransOpts = #{
        socket_opts => [{port, Port}],
        connection_type => supervisor
    },
    {ranch_tcp, TransOpts, cowboy_clear};
transport_settings(Port, SSLOpts) ->
    TransOpts = #{
        socket_opts => [
            {port, Port},
            {next_protocols_advertised, [<<"h2">>, <<"http/1.1">>]},
            {alpn_preferred_protocols, [<<"h2">>, <<"http/1.1">>]}
            | SSLOpts
        ],
        connection_type => supervisor
    },
    {ranch_ssl, TransOpts, cowboy_tls}.

default_handler(Req0, State) ->
    Req = cowboy_req:reply(
        400,
        #{<<"content-type">> => <<"text/plain">>},
        <<"">>,
        Req0
    ),
    {ok, Req, State}.

pick_port_number(Port) ->
    case is_port_in_use(Port) of
        true ->
            pick_port_number(Port + 1);
        false ->
            Port
    end.

is_port_in_use(Port) ->
    case gen_tcp:listen(Port, [{reuseaddr, true}, {active, false}]) of
        {ok, ListenSocket} ->
            gen_tcp:close(ListenSocket),
            false;
        {error, eaddrinuse} ->
            true
    end.
