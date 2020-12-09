%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Stomp Transaction

-module(emqx_stomp_transaction).

-include("emqx_stomp.hrl").

-export([ start/2
        , add/2
        , commit/2
        , abort/1
        , timeout/1
        ]).

-record(transaction, {id, actions, tref}).

-define(TIMEOUT, 60000).

start(Id, TimeoutMsg) ->
    case get({transaction, Id}) of
        undefined    ->
            TRef = erlang:send_after(?TIMEOUT, self(), TimeoutMsg),
            Transaction = #transaction{id = Id, actions = [], tref = TRef},
            put({transaction, Id}, Transaction),
            {ok, Transaction};
        _Transaction ->
            {error, already_started}
    end.

add(Id, Action) ->
    Fun = fun(Transaction = #transaction{actions = Actions}) ->
            Transaction1 = Transaction#transaction{actions = [Action | Actions]},
            put({transaction, Id}, Transaction1),
            {ok, Transaction1}
          end,
    with_transaction(Id, Fun).

commit(Id, InitState) ->
    Fun = fun(Transaction = #transaction{actions = Actions}) ->
            done(Transaction),
            {ok, lists:foldr(fun(Action, State) -> Action(State) end,
                             InitState, Actions)}
          end,
    with_transaction(Id, Fun).

abort(Id) ->
    with_transaction(Id, fun done/1).

timeout(Id) ->
    erase({transaction, Id}).

done(#transaction{id = Id, tref = TRef}) ->
    erase({transaction, Id}),
    catch erlang:cancel_timer(TRef),
    ok.

with_transaction(Id, Fun) ->
    case get({transaction, Id}) of
        undefined   -> {error, not_found};
        Transaction -> Fun(Transaction)
    end.

