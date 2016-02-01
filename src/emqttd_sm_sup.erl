%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>. All Rights Reserved.
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
%%% @doc Session Manager Supervisor.
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%-----------------------------------------------------------------------------
-module(emqttd_sm_sup).

-behaviour(supervisor).

-include("emqttd.hrl").

-define(SM, emqttd_sm).

-define(HELPER, emqttd_sm_helper).

-define(TABS, [mqtt_transient_session,
               mqtt_persistent_session]).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% Create session tables
    create_session_tabs(),

    %% Helper
    StatsFun = emqttd_stats:statsfun('sessions/count', 'sessions/max'),
    Helper = {?HELPER, {?HELPER, start_link, [StatsFun]},
                permanent, 5000, worker, [?HELPER]},

    %% SM Pool Sup
    MFA = {?SM, start_link, []},
    PoolSup = emqttd_pool_sup:spec([?SM, hash, erlang:system_info(schedulers), MFA]),

    {ok, {{one_for_all, 10, 3600}, [Helper, PoolSup]}}.
    
create_session_tabs() ->
    Opts = [ordered_set, named_table, public,
               {write_concurrency, true}],
    [ets:new(Tab, Opts) || Tab <- ?TABS].

