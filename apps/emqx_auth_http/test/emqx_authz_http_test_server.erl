%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_http_test_server).

-behaviour(supervisor).
-behaviour(cowboy_handler).

% cowboy_server callbacks
-export([init/2]).

% supervisor callbacks
-export([init/1]).

% API
-export([
    start_link/2,
    stop/0,
    set_handler/1
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

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

default_handler(Req0, State) ->
    Req = cowboy_req:reply(
        400,
        #{<<"content-type">> => <<"text/plain">>},
        <<"">>,
        Req0
    ),
    {ok, Req, State}.
