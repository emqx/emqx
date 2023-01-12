-module(emqx_crl_cache_http_server).

-behaviour(gen_server).
-compile([nowarn_export_all, export_all]).

set_crl(CRLPem) ->
    ets:insert(?MODULE, {crl, CRLPem}).

%%--------------------------------------------------------------------
%% `gen_server' APIs
%%--------------------------------------------------------------------

start_link(Parent, BasePort, CRLPem, Opts) ->
    process_flag(trap_exit, true),
    stop_http(),
    timer:sleep(100),
    gen_server:start_link(?MODULE, {Parent, BasePort, CRLPem, Opts}, []).

init({Parent, BasePort, CRLPem, Opts}) ->
    Tab = ets:new(?MODULE, [named_table, ordered_set, public]),
    ets:insert(Tab, {crl, CRLPem}),
    ok = start_http(Parent, [{port, BasePort} | Opts]),
    Parent ! {self(), ready},
    {ok, #{parent => Parent}}.

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    stop_http().

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

stop(Pid) ->
    ok = gen_server:stop(Pid).

%%--------------------------------------------------------------------
%% Callbacks
%%--------------------------------------------------------------------

start_http(Parent, Opts) ->
    {ok, _Pid1} = cowboy:start_clear(http, Opts, #{
        env => #{dispatch => compile_router(Parent)}
    }),
    ok.

stop_http() ->
    cowboy:stop_listener(http),
    ok.

compile_router(Parent) ->
    {ok, _} = application:ensure_all_started(cowboy),
    cowboy_router:compile([
        {'_', [{'_', ?MODULE, #{parent => Parent}}]}
    ]).

init(Req, #{parent := Parent} = State) ->
    %% assert
    <<"GET">> = cowboy_req:method(Req),
    [{crl, CRLPem}] = ets:lookup(?MODULE, crl),
    Parent ! http_get,
    Reply = reply(Req, CRLPem),
    {ok, Reply, State}.

reply(Req, CRLPem) ->
    cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain">>}, CRLPem, Req).
