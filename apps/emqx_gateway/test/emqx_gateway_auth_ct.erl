%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_gateway_auth_ct).

-compile(nowarn_export_all).
-compile(export_all).

-behaviour(gen_server).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-import(
    emqx_gateway_test_utils,
    [
        request/2,
        request/3
    ]
).

-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

-define(CALL(Msg), gen_server:call(?MODULE, {?FUNCTION_NAME, Msg}, 15000)).

-define(AUTHN_HTTP_PORT, 37333).
-define(AUTHN_HTTP_PATH, "/auth").

-define(AUTHZ_HTTP_PORT, 38333).
-define(AUTHZ_HTTP_PATH, "/authz/[...]").

-define(GATEWAYS, [coap, lwm2m, mqttsn, stomp, exproto]).

-define(CONFS, [
    emqx_coap_SUITE,
    emqx_lwm2m_SUITE,
    emqx_sn_protocol_SUITE,
    emqx_stomp_SUITE,
    emqx_exproto_SUITE
]).

-record(state, {}).

-define(AUTHZ_HTTP_RESP(Result, Req),
    cowboy_req:reply(
        200,
        #{<<"content-type">> => <<"application/json">>},
        "{\"result\": \"" ++ atom_to_list(Result) ++ "\"}",
        Req
    )
).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

group_names(Auths) ->
    [{group, Auth} || Auth <- Auths].

init_groups(Suite, Auths) ->
    All = emqx_common_test_helpers:all(Suite),
    [{Auth, [], All} || Auth <- Auths].

start_auth(Name) ->
    ?CALL(Name).

stop_auth(Name) ->
    ?CALL(Name).

start() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:stop(?MODULE).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    {ok, #state{}}.

handle_call({start_auth, Name}, _From, State) ->
    on_start_auth(Name),
    {reply, ok, State};
handle_call({stop_auth, Name}, _From, State) ->
    on_stop_auth(Name),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%------------------------------------------------------------------------------
%% Authenticators
%%------------------------------------------------------------------------------

on_start_auth(authn_http) ->
    %% start test server
    {ok, _} = emqx_authn_http_test_server:start_link(?AUTHN_HTTP_PORT, ?AUTHN_HTTP_PATH),
    timer:sleep(1000),

    %% set authn for gateway
    Setup = fun(Gateway) ->
        Path = io_lib:format("/gateways/~ts/authentication", [Gateway]),
        {204, _} = request(delete, Path),
        timer:sleep(200),
        {201, _} = request(post, Path, http_authn_config()),
        timer:sleep(200)
    end,
    lists:foreach(Setup, ?GATEWAYS),

    %% set handler for test server
    Handler = fun(Req0, State) ->
        ct:pal("Authn Req:~p~nState:~p~n", [Req0, State]),
        Headers = #{<<"content-type">> => <<"application/json">>},
        Response = emqx_utils_json:encode(#{result => allow, is_superuser => false}),
        case cowboy_req:match_qs([username, password], Req0) of
            #{
                username := <<"admin">>,
                password := <<"public">>
            } ->
                Req = cowboy_req:reply(200, Headers, Response, Req0);
            _ ->
                Req = cowboy_req:reply(400, Req0)
        end,
        {ok, Req, State}
    end,
    emqx_authn_http_test_server:set_handler(Handler),

    timer:sleep(500);
on_start_auth(authz_http) ->
    ok = emqx_authz_test_lib:reset_authorizers(),
    {ok, _} = emqx_authz_http_test_server:start_link(?AUTHZ_HTTP_PORT, ?AUTHZ_HTTP_PATH),

    %% TODO set authz for gateway
    ok = emqx_authz_test_lib:setup_config(
        http_authz_config(),
        #{}
    ),

    %% set handler for test server
    Handler = fun(Req0, State) ->
        case cowboy_req:match_qs([topic, action, username], Req0) of
            #{topic := <<"/publish">>, action := <<"publish">>} ->
                Req = ?AUTHZ_HTTP_RESP(allow, Req0);
            #{topic := <<"/subscribe">>, action := <<"subscribe">>} ->
                Req = ?AUTHZ_HTTP_RESP(allow, Req0);
            %% for lwm2m
            #{username := <<"lwm2m">>} ->
                Req = ?AUTHZ_HTTP_RESP(allow, Req0);
            _ ->
                Req = cowboy_req:reply(400, Req0)
        end,
        {ok, Req, State}
    end,
    ok = emqx_authz_http_test_server:set_handler(Handler),
    timer:sleep(500).

on_stop_auth(authn_http) ->
    Delete = fun(Gateway) ->
        Path = io_lib:format("/gateways/~ts/authentication", [Gateway]),
        {204, _} = request(delete, Path)
    end,
    lists:foreach(Delete, ?GATEWAYS),
    ok = emqx_authn_http_test_server:stop();
on_stop_auth(authz_http) ->
    ok = emqx_authz_http_test_server:stop().

%%------------------------------------------------------------------------------
%% Configs
%%------------------------------------------------------------------------------

http_authn_config() ->
    #{
        <<"mechanism">> => <<"password_based">>,
        <<"enable">> => <<"true">>,
        <<"backend">> => <<"http">>,
        <<"method">> => <<"get">>,
        <<"url">> => <<"http://127.0.0.1:37333/auth">>,
        <<"body">> => #{<<"username">> => ?PH_USERNAME, <<"password">> => ?PH_PASSWORD},
        <<"headers">> => #{<<"X-Test-Header">> => <<"Test Value">>}
    }.

http_authz_config() ->
    #{
        <<"enable">> => <<"true">>,
        <<"type">> => <<"http">>,
        <<"method">> => <<"get">>,
        <<"url">> =>
            <<"http://127.0.0.1:38333/authz/users/?topic=${topic}&action=${action}&username=${username}">>,
        <<"headers">> => #{<<"X-Test-Header">> => <<"Test Value">>}
    }.

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

init_gateway_conf() ->
    ok = emqx_common_test_helpers:load_config(
        emqx_gateway_schema,
        merge_conf(list_gateway_conf(), [])
    ).

list_gateway_conf() ->
    [X:default_config() || X <- ?CONFS].

merge_conf([Conf | T], Acc) ->
    case re:run(Conf, "\s*gateway\\.(.*)", [global, {capture, all_but_first, list}, dotall]) of
        {match, [[Content]]} ->
            merge_conf(T, [Content | Acc]);
        _ ->
            merge_conf(T, Acc)
    end;
merge_conf([], Acc) ->
    erlang:list_to_binary("gateway{" ++ string:join(Acc, ",") ++ "}").

with_resource(Init, Close, Fun) ->
    Res =
        case Init() of
            {ok, X} -> X;
            Other -> Other
        end,
    try
        Fun(Res)
    catch
        C:R:S ->
            erlang:raise(C, R, S)
    after
        Close(Res)
    end.
