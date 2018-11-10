-module(ws_client).

-export([
         start_link/0,
         start_link/1,
         send_binary/2,
         send_ping/2,
         recv/2,
         recv/1,
         stop/1
        ]).

-export([
         init/2,
         websocket_handle/3,
         websocket_info/3,
         websocket_terminate/3
        ]).

-record(state, {
          buffer = [] :: list(),
          waiting = undefined :: undefined | pid()
         }).

start_link() ->
    start_link("ws://localhost:8083/mqtt").

start_link(Url) ->
    websocket_client:start_link(Url, ?MODULE, [], [{extra_headers, [{"Sec-Websocket-Protocol", "mqtt"}]}]).

stop(Pid) ->
    Pid ! stop.

send_binary(Pid, Msg) ->
    websocket_client:cast(Pid, {binary, Msg}).

send_ping(Pid, Msg) ->
    websocket_client:cast(Pid, {ping, Msg}).

recv(Pid) ->
    recv(Pid, 5000).

recv(Pid, Timeout) ->
    Pid ! {recv, self()},
    receive
        M -> M
    after
        Timeout -> error
    end.

init(_, _WSReq) ->
    {ok, #state{}}.

websocket_handle(Frame, _, State = #state{waiting = undefined, buffer = Buffer}) ->
    logger:info("Client received frame~p", [Frame]),
    {ok, State#state{buffer = [Frame|Buffer]}};
websocket_handle(Frame, _, State = #state{waiting = From}) ->
    logger:info("Client received frame~p", [Frame]),
    From ! Frame,
    {ok, State#state{waiting = undefined}}.

websocket_info({send_text, Text}, WSReq, State) ->
    websocket_client:send({text, Text}, WSReq),
    {ok, State};
websocket_info({recv, From}, _, State = #state{buffer = []}) ->
    {ok, State#state{waiting = From}};
websocket_info({recv, From}, _, State = #state{buffer = [Top|Rest]}) ->
    From ! Top,
    {ok, State#state{buffer = Rest}};
websocket_info(stop, _, State) ->
    {close, <<>>, State}.

websocket_terminate(Close, _, State) ->
    io:format("Websocket closed with frame ~p and state ~p", [Close, State]),
    ok.
