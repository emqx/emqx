%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mqttsn_broadcast).

-behaviour(gen_server).

-include("emqx_mqttsn.hrl").
-include_lib("emqx/include/logger.hrl").

-export([
    start_link/2,
    stop/0
]).

%% gen_server
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {gwid, sock, port, addrs, duration, tref}).

-define(DEFAULT_DURATION, 15 * 60 * 1000).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link(pos_integer(), inet:port_number()) ->
    {ok, pid()} | {error, term()}.
start_link(GwId, Port) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [GwId, Port], []).

-spec stop() -> ok.
stop() ->
    gen_server:stop(?MODULE, nomal, infinity).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([GwId, Port]) ->
    %% FIXME:
    Duration = application:get_env(emqx_mqttsn, advertise_duration, ?DEFAULT_DURATION),
    {ok, Sock} = gen_udp:open(0, [binary, {broadcast, true}]),
    {ok,
        ensure_advertise(#state{
            gwid = GwId,
            addrs = boradcast_addrs(),
            sock = Sock,
            port = Port,
            duration = Duration
        })}.

handle_call(Req, _From, State) ->
    ?SLOG(error, #{
        msg => "unexpected_call",
        call => Req
    }),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{
        msg => "unexpected_cast",
        cast => Msg
    }),
    {noreply, State}.

handle_info(broadcast_advertise, State) ->
    {noreply, ensure_advertise(State), hibernate};
handle_info(Info, State) ->
    ?SLOG(error, #{
        msg => "unexpected_info",
        info => Info
    }),
    {noreply, State}.

terminate(_Reason, #state{tref = Timer}) ->
    _ = erlang:cancel_timer(Timer),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

ensure_advertise(State = #state{duration = Duration}) ->
    send_advertise(State),
    State#state{tref = erlang:send_after(Duration, self(), broadcast_advertise)}.

send_advertise(#state{
    gwid = GwId,
    sock = Sock,
    port = Port,
    addrs = Addrs,
    duration = Duration
}) ->
    Data = emqx_mqttsn_frame:serialize_pkt(?SN_ADVERTISE_MSG(GwId, Duration), #{}),
    lists:foreach(
        fun(Addr) ->
            ?SLOG(debug, #{
                msg => "send_ADVERTISE_msg",
                address => Addr
            }),
            gen_udp:send(Sock, Addr, Port, Data)
        end,
        Addrs
    ).

boradcast_addrs() ->
    lists:usort([
        Addr
     || {ok, IfList} <- [inet:getiflist()],
        If <- IfList,
        {ok, [{broadaddr, Addr}]} <- [inet:ifget(If, [broadaddr])]
    ]).
