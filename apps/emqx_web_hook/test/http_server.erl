%%--------------------------------------------------------------------
%% A Simple HTTP Server based cowboy
%%
%% It will deliver the http-request params to initialer process
%%--------------------------------------------------------------------
%%
%% Author:wwhai
%%
-module(http_server).
-behaviour(gen_server).

-export([start_link/0]).
-export([get_received_data/0]).
-export([stop/1]).
-export([code_change/3, handle_call/3, handle_cast/2, handle_info/2, init/1, init/2, terminate/2]).
-define(HTTP_PORT, 9999).
-define(HTTPS_PORT, 8888).
-record(state, {}).
%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link() ->
    stop_http(),
    stop_https(),
    timer:sleep(100),
    gen_server:start_link(?MODULE, [], []).

init([]) ->
    EtsOptions = [named_table, public, set, {write_concurrency, true},
                                            {read_concurrency, true}],
    emqx_web_hook_http_test = ets:new(emqx_web_hook_http_test, EtsOptions),
    ok = start_http(?HTTP_PORT),
    ok = start_https(?HTTPS_PORT),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    stop_http(),
    stop_https().

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

get_received_data() ->
    ets:tab2list(emqx_web_hook_http_test).

stop(Pid) ->
    ok = gen_server:stop(Pid).

%%--------------------------------------------------------------------
%% Callbacks
%%--------------------------------------------------------------------

start_http(Port) ->
    {ok, _Pid1} = cowboy:start_clear(http, [{port, Port}], #{
        env => #{dispatch => compile_router()}
    }),
    io:format(standard_error, "[TEST LOG] Start http server on 9999 successfully!~n", []).

start_https(Port) ->
    Path = emqx_ct_helpers:deps_path(emqx_web_hook, "test/emqx_web_hook_SUITE_data/"),
    SslOpts = [{keyfile, Path ++ "/server-key.pem"},
               {cacertfile, Path ++ "/ca.pem"},
               {certfile, Path ++ "/server-cert.pem"}],

    {ok, _Pid2} = cowboy:start_tls(https, [{port, Port}] ++ SslOpts,
                                   #{env => #{dispatch => compile_router()}}),
    io:format(standard_error, "[TEST LOG] Start https server on 8888 successfully!~n", []).

stop_http() ->
    cowboy:stop_listener(http),
    io:format("[TEST LOG] Stopped http server on 9999").

stop_https() ->
    cowboy:stop_listener(https),
    io:format("[TEST LOG] Stopped https server on 8888").

compile_router() ->
    {ok, _} = application:ensure_all_started(cowboy),
    cowboy_router:compile([
        {'_', [{"/", ?MODULE, #{}}]}
    ]).

init(Req, State) ->
    Method = cowboy_req:method(Req),
    Headers = cowboy_req:headers(Req),
    [Params] = case Method of
                 <<"GET">> -> cowboy_req:parse_qs(Req);
                 <<"POST">> ->
                     {ok, PostVals, _} = cowboy_req:read_urlencoded_body(Req),
                     PostVals
             end,
    ets:insert(emqx_web_hook_http_test, {Params, Headers}),
    {ok, reply(Req, ok), State}.

reply(Req, ok) ->
    cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain">>}, <<"ok">>, Req);
reply(Req, error) ->
    cowboy_req:reply(404, #{<<"content-type">> => <<"text/plain">>}, <<"deny">>, Req).
