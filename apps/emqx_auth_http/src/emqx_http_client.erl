-module(emqx_http_client).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").

%% APIs
-export([ start_link/3
        , request/3
        , request/4
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {
          pool      :: ecpool:poo_name(),
          id        :: pos_integer(),
          client    :: pid() | undefined,
          mref      :: reference() | undefined,
          host      :: inet:hostname() | inet:ip_address(),
          port      :: inet:port_number(),
          gun_opts  :: proplists:proplist(),
          gun_state :: down | up,
          requests  :: map()
         }).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link(Pool, Id, Opts) ->
    gen_server:start_link(?MODULE, [Pool, Id, Opts], []).

request(Method, Pool, Req) ->
    request(Method, Pool, Req, 5000).

request(get, Pool, {Path, Headers}, Timeout) ->
    call(pick(Pool), {get, {Path, Headers}, Timeout}, Timeout + 1000);
request(Method, Pool, {Path, Headers, Body}, Timeout) ->
    call(pick(Pool), {Method, {Path, Headers, Body}, Timeout}, Timeout + 1000).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Pool, Id, Opts]) ->
    State = #state{pool = Pool,
                   id = Id,
                   client = undefined,
                   mref = undefined,
                   host = proplists:get_value(host, Opts),
                   port = proplists:get_value(port, Opts),
                   gun_opts = gun_opts(Opts),
                   gun_state = down,
                   requests = #{}},
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, State}.

handle_call(Req = {_, _, _}, From, State = #state{client = undefined, gun_state = down}) ->
    case open(State) of
        {ok, NewState} ->
            handle_call(Req, From, NewState);
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call(Req = {_, _, Timeout}, From, State = #state{client = Client, mref = MRef, gun_state = down}) when is_pid(Client) ->
    case gun:await_up(Client, Timeout, MRef) of
        {ok, _} ->
            handle_call(Req, From, State#state{gun_state = up});
        {error, timeout} ->
            {reply, {error, timeout}, State};
        {error, Reason} ->
            true = erlang:demonitor(MRef, [flush]),
            {reply, {error, Reason}, State#state{client = undefined, mref = undefined}}
    end;

handle_call({Method, Request, Timeout}, From, State = #state{client = Client, requests = Requests, gun_state = up}) when is_pid(Client) ->
    StreamRef = do_request(Client, Method, Request),
    ExpirationTime = erlang:system_time(millisecond) + Timeout,
    {noreply, State#state{requests = maps:put(StreamRef, {From, ExpirationTime, undefined}, Requests)}};

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({gun_response, Client, StreamRef, IsFin, StatusCode, Headers}, State = #state{client = Client, requests = Requests}) ->
    Now = erlang:system_time(millisecond),
    case maps:take(StreamRef, Requests) of
        error ->
            ?LOG(error, "Received 'gun_response' message from unknown stream ref: ~p", [StreamRef]),
            {noreply, State};
        {{_, ExpirationTime, _}, NRequests} when Now > ExpirationTime ->
            gun:cancel(Client, StreamRef),
            flush_stream(Client, StreamRef),
            {noreply, State#state{requests = NRequests}};
        {{From, ExpirationTime, undefined}, NRequests} ->
            case IsFin of
                fin ->
                    gen_server:reply(From, {ok, StatusCode, Headers}),
                    {noreply, State#state{requests = NRequests}};
                nofin ->
                    {noreply, State#state{requests = NRequests#{StreamRef => {From, ExpirationTime, {StatusCode, Headers, <<>>}}}}}
            end;
        _ ->
            ?LOG(error, "Received 'gun_response' message does not match the state"),
            {noreply, State}
    end;

handle_info({gun_data, Client, StreamRef, IsFin, Data}, State = #state{client = Client, requests = Requests}) ->
    Now = erlang:system_time(millisecond),
    case maps:take(StreamRef, Requests) of
        error ->
            ?LOG(error, "Received 'gun_data' message from unknown stream ref: ~p", [StreamRef]),
            {noreply, State};
        {{_, ExpirationTime, _}, NRequests} when Now > ExpirationTime ->
            gun:cancel(Client, StreamRef),
            flush_stream(Client, StreamRef),
            {noreply, State#state{requests = NRequests}};
        {{From, ExpirationTime, {StatusCode, Headers, Acc}}, NRequests} ->
            case IsFin of
                fin ->
                    gen_server:reply(From, {ok, StatusCode, Headers, <<Acc/binary, Data/binary>>}),
                    {noreply, State#state{requests = NRequests}};
                nofin ->
                    {noreply, State#state{requests = NRequests#{StreamRef => {From, ExpirationTime, {StatusCode, Headers, <<Acc/binary, Data/binary>>}}}}}
            end;
        _ ->
            ?LOG(error, "Received 'gun_data' message does not match the state"),
            {noreply, State}
    end;

handle_info({gun_error, Client, StreamRef, Reason}, State = #state{client = Client, requests = Requests}) ->
    Now = erlang:system_time(millisecond),
    case maps:take(StreamRef, Requests) of
        error ->
            ?LOG(error, "Received 'gun_error' message from unknown stream ref: ~p~n", [StreamRef]),
            {noreply, State};
        {{_, ExpirationTime, _}, NRequests} when Now > ExpirationTime ->
            {noreply, State#state{requests = NRequests}};
        {{From, _, _}, NRequests} ->
            gen_server:reply(From, {error, Reason}),
            {noreply, State#state{requests = NRequests}}
    end;

handle_info({gun_up, Client, _}, State = #state{client = Client}) ->
    {noreply, State#state{gun_state = up}};

handle_info({gun_down, Client, _, Reason, KilledStreams, _}, State = #state{client = Client, requests = Requests}) ->
    Now = erlang:system_time(millisecond),
    NRequests = lists:foldl(fun(StreamRef, Acc) ->
                                case maps:take(StreamRef, Acc) of
                                    error -> Acc;
                                    {{_, ExpirationTime, _}, NAcc} when Now > ExpirationTime ->
                                        NAcc;
                                    {{From, _, _}, NAcc} ->
                                        gen_server:reply(From, {error, Reason}),
                                        NAcc
                                end
                            end, Requests, KilledStreams),
    {noreply, State#state{gun_state = down, requests = NRequests}};

handle_info({'DOWN', MRef, process, Client, Reason}, State = #state{mref = MRef, client = Client, requests = Requests}) ->
    true = erlang:demonitor(MRef, [flush]),
    Now = erlang:system_time(millisecond),
    lists:foreach(fun({_, {_, ExpirationTime, _}}) when Now > ExpirationTime ->
                      ok;
                     ({_, {From, _, _}}) ->
                      gen_server:reply(From, {error, Reason})
                  end, maps:to_list(Requests)),
    case open(State#state{requests = #{}}) of
        {ok, NewState} ->
            {noreply, NewState};
        {error, Reason} ->
            {noreply, State#state{mref = undefined, client = undefined}}
    end;

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{pool = Pool, id = Id}) ->
    gropc:disconnect_worker(Pool, {Pool, Id}),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

open(State = #state{host = Host, port = Port, gun_opts = GunOpts}) ->
    case gun:open(Host, Port, GunOpts) of
        {ok, ConnPid} when is_pid(ConnPid) ->
            MRef = monitor(process, ConnPid),
            {ok, State#state{mref = MRef, client = ConnPid}};
        {error, Reason} ->
            {error, Reason}
    end.

gun_opts(Opts) ->
    gun_opts(Opts, #{retry => 5,
                     retry_timeout => 1000,
                     connect_timeout => 5000,
                     protocols => [http],
                     http_opts => #{keepalive => infinity}}).

gun_opts([], Acc) ->
    Acc;
gun_opts([{retry, Retry} | Opts], Acc) ->
    gun_opts(Opts, Acc#{retry => Retry});
gun_opts([{retry_timeout, RetryTimeout} | Opts], Acc) ->
    gun_opts(Opts, Acc#{retry_timeout => RetryTimeout});
gun_opts([{connect_timeout, ConnectTimeout} | Opts], Acc) ->
    gun_opts(Opts, Acc#{connect_timeout => ConnectTimeout});
gun_opts([{transport, Transport} | Opts], Acc) ->
    gun_opts(Opts, Acc#{transport => Transport});
gun_opts([{transport_opts, TransportOpts} | Opts], Acc) ->
    gun_opts(Opts, Acc#{transport_opts => TransportOpts});
gun_opts([_ | Opts], Acc) ->
    gun_opts(Opts, Acc).

call(ChannPid, Msg, Timeout) ->
    gen_server:call(ChannPid, Msg, Timeout).

pick(Pool) ->
    gproc_pool:pick_worker(Pool).

do_request(Client, get, {Path, Headers}) ->
    gun:get(Client, Path, Headers);
do_request(Client, post, {Path, Headers, Body}) ->
    gun:post(Client, Path, Headers, Body).

flush_stream(Client, StreamRef) ->
    receive
        {gun_response, Client, StreamRef, _, _, _} ->
            flush_stream(Client, StreamRef);
        {gun_data, Client, StreamRef, _, _} ->
            flush_stream(Client, StreamRef);
        {gun_error, Client, StreamRef, _} ->
            flush_stream(Client, StreamRef)
	after 0 ->
		ok
	end.