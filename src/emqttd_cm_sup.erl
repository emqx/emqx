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
%%% @doc Client Manager Supervisor.
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%
%%%-----------------------------------------------------------------------------
-module(emqttd_cm_sup).

-behaviour(supervisor).

-include("emqttd.hrl").

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(CM, emqttd_cm).

-define(TAB, mqtt_client).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% Create client table
    create_client_tab(),

    %% CM Pool Sup
    MFA = {?CM, start_link, [emqttd_stats:statsfun('clients/count', 'clients/max')]},
    PoolSup = emqttd_pool_sup:spec(pool_sup, [?CM, hash, erlang:system_info(schedulers), MFA]),

    {ok, {{one_for_all, 10, 3600}, [PoolSup]}}.

create_client_tab() ->
    case ets:info(?TAB, name) of
        undefined ->
            ets:new(?TAB, [ordered_set, named_table, public,
                           {keypos, 2}, {write_concurrency, true}]);
        _ ->
            ok
    end.

