%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% emqttd supervisor.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_sup).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-behaviour(supervisor).

%% API
-export([start_link/0, start_child/1, start_child/2]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%%%=============================================================================
%%% API
%%%=============================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(ChildSpec) when is_tuple(ChildSpec) ->
	supervisor:start_child(?MODULE, ChildSpec).

%%
%% start_child(Mod::atom(), Type::type()) -> {ok, pid()}
%% @type type() = worker | supervisor
%%
start_child(Mod, Type) when is_atom(Mod) and is_atom(Type) ->
	supervisor:start_child(?MODULE, ?CHILD(Mod, Type)).

%%%=============================================================================
%%% Supervisor callbacks
%%%=============================================================================

init([]) ->
    {ok, {{one_for_all, 10, 100}, []}}.

