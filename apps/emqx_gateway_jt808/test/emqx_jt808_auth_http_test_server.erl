%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_jt808_auth_http_test_server).

-behaviour(supervisor).
-behaviour(cowboy_handler).

% cowboy_server callbacks
-export([init/2]).

% supervisor callbacks
-export([init/1]).

% API
-export([
    start_link/0, start_link/1, start_link/2,
    stop/0,
    set_handler/1
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link() ->
    start_link(8991).

start_link(Port) ->
    start_link(Port, "/[...]").

start_link(Port, Path) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Port, Path]).

stop() ->
    gen_server:stop(?MODULE).

set_handler(F) when is_function(F, 2) ->
    true = ets:insert(?MODULE, {handler, F}),
    ok.

%%------------------------------------------------------------------------------
%% supervisor API
%%------------------------------------------------------------------------------

init([Port, Path]) ->
    Dispatch = cowboy_router:compile(
        [
            {'_', [{Path, ?MODULE, []}]}
        ]
    ),
    TransOpts = #{
        socket_opts => [{port, Port}],
        connection_type => supervisor
    },
    ProtoOpts = #{env => #{dispatch => Dispatch}},

    Tab = ets:new(?MODULE, [set, named_table, public]),
    ets:insert(Tab, {handler, fun default_handler/2}),

    ChildSpec = ranch:child_spec(?MODULE, ranch_tcp, TransOpts, cowboy_clear, ProtoOpts),
    {ok, {{one_for_one, 10, 10}, [ChildSpec]}}.

%%------------------------------------------------------------------------------
%% cowboy_server API
%%------------------------------------------------------------------------------

init(Req, State) ->
    [{handler, Handler}] = ets:lookup(?MODULE, handler),
    Handler(Req, State).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

default_handler(Req0 = #{method := <<"POST">>, path := <<"/jt808/registry">>}, State) ->
    Req = cowboy_req:reply(
        200,
        #{<<"content-type">> => <<"application/json">>},
        emqx_utils_json:encode(#{code => 0, authcode => <<"123456">>}),
        Req0
    ),
    {ok, Req, State};
default_handler(Req0 = #{method := <<"POST">>, path := <<"/jt808/auth">>}, State) ->
    Req = cowboy_req:reply(
        200,
        #{<<"content-type">> => <<"application/json">>},
        emqx_utils_json:encode(#{client_id => <<"abcdef">>}),
        Req0
    ),
    {ok, Req, State};
default_handler(Req0, State) ->
    Req = cowboy_req:reply(
        400,
        #{<<"content-type">> => <<"text/plain">>},
        <<"">>,
        Req0
    ),
    {ok, Req, State}.
