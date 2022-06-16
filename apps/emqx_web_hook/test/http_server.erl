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

-export([start_link/3]).
-export([stop/1]).
-export([code_change/3, handle_call/3, handle_cast/2, handle_info/2, init/1, init/2, terminate/2]).
-record(state, {parent :: pid()}).
%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link(Parent, BasePort, Opts) ->
    stop_http(),
    stop_https(),
    timer:sleep(100),
    gen_server:start_link(?MODULE, {Parent, BasePort, Opts}, []).

init({Parent, BasePort, Opts}) ->
    ok = start_http(Parent, [{port, BasePort} | Opts]),
    ok = start_https(Parent, [{port, BasePort + 1} | Opts]),
    Parent ! {self(), ready},
    {ok, #state{parent = Parent}}.

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

stop(Pid) ->
    ok = gen_server:stop(Pid).

%%--------------------------------------------------------------------
%% Callbacks
%%--------------------------------------------------------------------

start_http(Parent, Opts) ->
    {ok, _Pid1} = cowboy:start_clear(http, Opts, #{
        env => #{dispatch => compile_router(Parent)}
    }),
    Port = proplists:get_value(port, Opts),
    io:format(standard_error, "[TEST LOG] Start http server on ~p successfully!~n", [Port]).

start_https(Parent, Opts) ->
    Path = emqx_ct_helpers:deps_path(emqx_web_hook, "test/emqx_web_hook_SUITE_data/"),
    SslOpts = [{keyfile, Path ++ "/server-key.pem"},
               {cacertfile, Path ++ "/ca.pem"},
               {certfile, Path ++ "/server-cert.pem"}],

    {ok, _Pid2} = cowboy:start_tls(https, Opts ++ SslOpts,
                                   #{env => #{dispatch => compile_router(Parent)}}),
    Port = proplists:get_value(port, Opts),
    io:format(standard_error, "[TEST LOG] Start https server on ~p successfully!~n", [Port]).

stop_http() ->
    cowboy:stop_listener(http),
    io:format("[TEST LOG] Stopped http server").

stop_https() ->
    cowboy:stop_listener(https),
    io:format("[TEST LOG] Stopped https server").

compile_router(Parent) ->
    {ok, _} = application:ensure_all_started(cowboy),
    cowboy_router:compile([
        {'_', [{'_', ?MODULE, #{parent => Parent}}]}
    ]).

init(Req, #{parent := Parent} = State) ->
    Method = cowboy_req:method(Req),
    Headers = cowboy_req:headers(Req),
    [Params] = case Method of
                 <<"GET">> -> cowboy_req:parse_qs(Req);
                 <<"POST">> ->
                     {ok, PostVals, _} = cowboy_req:read_urlencoded_body(Req),
                     PostVals
             end,
    Parent ! {?MODULE, Params, Headers},
    {ok, reply(Req, ok), State}.

reply(Req, ok) ->
    cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain">>}, <<"ok">>, Req);
reply(Req, error) ->
    cowboy_req:reply(404, #{<<"content-type">> => <<"text/plain">>}, <<"deny">>, Req).
