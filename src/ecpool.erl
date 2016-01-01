%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2015 eMQTT.IO, All Rights Reserved.
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
%%% @doc ecpool API.
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%-----------------------------------------------------------------------------

-module(ecpool).

-export([name/1, start_pool/3, with_client/2, with_client/3, stop_pool/1]).

name(Pool) ->
    {?MODULE, Pool}.

start_pool(Pool, Mod, Opts) when is_atom(Pool) ->
    ecpool_pool_sup:start_link(Pool, Mod, Opts).

stop_pool(Pool) when is_atom(Pool) ->
    ecpool_sup:stop_pool(Pool).

with_client(Pool, Fun) when is_atom(Pool) ->
    Worker = gproc_pool:pick_worker({?MODULE, Pool}),
    with_worker(Worker, Fun).

with_client(Pool, Key, Fun) when is_atom(Pool) ->
    Worker = gproc_pool:pick_worker({?MODULE, Pool}, Key),
    with_worker(Worker, Fun).

with_worker(Worker, Fun) ->
    case ecpool_worker:client(Worker) of
        {ok, Client}    -> Fun(Client);
        {error, Reason} -> {error, Reason}
    end.

