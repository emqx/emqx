-module(emqx_acme_challenge_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [
        t_serve_challenge,
        t_serve_unknown_token_returns_404
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(cowboy),
    {ok, _} = application:ensure_all_started(inets),
    %% The challenge responder is the gen_server that owns the ETS
    %% table and the cowboy listener; in production it's started under
    %% emqx_acme_sup, but here we run it standalone so the suite
    %% doesn't need to bring up the whole plugin. start_link links it
    %% to init_per_suite's transient CT process — unlink right away so
    %% the responder outlives that process and survives across cases.
    {ok, Pid} = emqx_acme_challenge:start_link(),
    true = erlang:unlink(Pid),
    [{responder, Pid} | Config].

end_per_suite(Config) ->
    Pid = proplists:get_value(responder, Config),
    case is_process_alive(Pid) of
        true -> gen_server:stop(Pid);
        false -> ok
    end,
    ok.

end_per_testcase(_Case, _Config) ->
    %% Tests share the same gen_server; stop the listener between
    %% cases so each test gets a fresh bind.
    catch emqx_acme_challenge:stop_listener(),
    ok.

-doc "After set_challenges/1 the cowboy handler returns the keyAuth body for "
"GET /.well-known/acme-challenge/<token> with content-type "
"application/octet-stream and HTTP 200.".
t_serve_challenge(_Config) ->
    Port = pick_free_port(),
    ok = emqx_acme_challenge:start_listener(Port),
    ok = emqx_acme_challenge:set_challenges(
        [#{token => <<"tok-1">>, key => <<"keyauth-1">>}]
    ),
    Url = url(Port, "tok-1"),
    {ok, {{_Vsn, 200, _Reason}, _Headers, Body}} = httpc:request(Url),
    ?assertEqual(<<"keyauth-1">>, iolist_to_binary(Body)).

-doc "An unknown challenge token returns HTTP 404 with body \"not found\".".
t_serve_unknown_token_returns_404(_Config) ->
    Port = pick_free_port(),
    ok = emqx_acme_challenge:start_listener(Port),
    ok = emqx_acme_challenge:set_challenges([]),
    Url = url(Port, "ghost"),
    {ok, {{_Vsn, 404, _Reason}, _Headers, Body}} = httpc:request(Url),
    ?assertEqual(<<"not found">>, iolist_to_binary(Body)).

%% Helpers

pick_free_port() ->
    {ok, S} = gen_tcp:listen(0, [{reuseaddr, true}]),
    {ok, P} = inet:port(S),
    ok = gen_tcp:close(S),
    P.

url(Port, Token) ->
    "http://127.0.0.1:" ++ integer_to_list(Port) ++
        "/.well-known/acme-challenge/" ++ Token.
