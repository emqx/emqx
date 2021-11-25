%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_http_test_server).

-behaviour(gen_server).
-behaviour(cowboy_handler).

% cowboy_server callbacks
-export([init/2]).

% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2
        ]).

% API
-export([start/2,
         stop/0,
         set_handler/1
        ]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start(Port, Path) ->
    Dispatch = cowboy_router:compile([
        {'_', [{Path, ?MODULE, []}]}
    ]),
    {ok, _} = cowboy:start_clear(?MODULE,
        [{port, Port}],
        #{env => #{dispatch => Dispatch}}
    ),
    {ok, _} = gen_server:start_link({local, ?MODULE}, ?MODULE, [], []),
    ok.

stop() ->
    gen_server:stop(?MODULE),
    cowboy:stop_listener(?MODULE).

set_handler(F) when is_function(F, 2) ->
    gen_server:call(?MODULE, {set_handler, F}).

%%------------------------------------------------------------------------------
%% gen_server API
%%------------------------------------------------------------------------------

init([]) ->
    F = fun(Req0, State) ->
                Req = cowboy_req:reply(
                        400,
                        #{<<"content-type">> => <<"text/plain">>},
                        <<"">>,
                        Req0),
                {ok, Req, State}
        end,
    {ok, F}.

handle_cast(_, F) ->
    {noreply, F}.

handle_call({set_handler, F}, _From, _F) ->
    {reply, ok, F};

handle_call(get_handler, _From, F) ->
    {reply, F, F}.

%%------------------------------------------------------------------------------
%% cowboy_server API
%%------------------------------------------------------------------------------

init(Req, State) ->
    Handler = gen_server:call(?MODULE, get_handler),
    Handler(Req, State).
