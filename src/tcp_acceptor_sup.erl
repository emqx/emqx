%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(tcp_acceptor_sup).

-behaviour(supervisor).

-export([start_link/2]).

-export([init/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(mfargs() :: {atom(), atom(), [any()]}).

-spec(start_link/2 :: (atom(), mfargs()) -> rabbit_types:ok_pid_or_error()).

-endif.

%%----------------------------------------------------------------------------

start_link(Name, Callback) ->
    supervisor:start_link({local,Name}, ?MODULE, Callback).

init(Callback) ->
    {ok, {{simple_one_for_one, 10, 10},
          [{tcp_acceptor, {tcp_acceptor, start_link, [Callback]},
            transient, brutal_kill, worker, [tcp_acceptor]}]}}.
