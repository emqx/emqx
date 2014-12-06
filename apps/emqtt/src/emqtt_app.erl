%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is eMQTT
%%
%% The Initial Developer of the Original Code is <ery.lee at gmail dot com>
%% Copyright (C) 2012 Ery Lee All Rights Reserved.

-module(emqtt_app).

-author('ery.lee@gmail.com').

-include("emqtt.hrl").

-include_lib("elog/include/elog.hrl").

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

%%
%% @spec start(atom(), list()) -> {ok, pid()}
%%
start(_StartType, _StartArgs) ->
	?INFO("starting emqtt on node '~s'", [node()]),
	{ok, Listeners} = application:get_env(listeners),
    {ok, SupPid} = emqtt_sup:start_link(Listeners),
	register(emqtt, self()),
	?INFO_MSG("emqtt broker is running now."),
	{ok, SupPid}.

%%
%% @spec stop(atom) -> 'ok'
%%
stop(_State) ->
    ok.

