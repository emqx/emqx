%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_coap_observe_res).

-include("emqx_coap.hrl").

%% API
-export([
    new_manager/0,
    insert/2,
    remove/2,
    res_changed/2,
    foreach/2,
    subscriptions/1
]).
-export_type([manager/0]).

-define(MAX_SEQ_ID, 16777215).

-type token() :: binary().
-type seq_id() :: 0..?MAX_SEQ_ID.

-type res() :: #{
    token := token(),
    seq_id := seq_id(),
    subopts := emqx_types:subopts()
}.

-type manager() :: #{emqx_types:topic() => res()}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
-spec new_manager() -> manager().
new_manager() ->
    #{}.

-spec insert(sub_data(), manager()) -> {seq_id(), manager()}.
insert(#{topic := Topic, token := Token, subopts := SubOpts}, Manager) ->
    Res =
        case maps:get(Topic, Manager, undefined) of
            undefined ->
                new_res(Token, SubOpts);
            Any ->
                Any
        end,
    {maps:get(seq_id, Res), Manager#{Topic => Res}}.

-spec remove(emqx_types:topic(), manager()) -> manager().
remove(Topic, Manager) ->
    maps:remove(Topic, Manager).

-spec res_changed(emqx_types:topic(), manager()) -> undefined | {token(), seq_id(), manager()}.
res_changed(Topic, Manager) ->
    case maps:get(Topic, Manager, undefined) of
        undefined ->
            undefined;
        Res ->
            #{
                token := Token,
                seq_id := SeqId
            } = Res2 = res_changed(Res),
            {Token, SeqId, Manager#{Topic := Res2}}
    end.

foreach(F, Manager) ->
    maps:fold(
        fun(K, V, _) ->
            F(K, V)
        end,
        ok,
        Manager
    ),
    ok.

-spec subscriptions(manager()) -> _.
subscriptions(Manager) ->
    maps:map(
        fun(_Topic, #{subopts := SubOpts}) ->
            SubOpts
        end,
        Manager
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
-spec new_res(token(), emqx_types:subopts()) -> res().
new_res(Token, SubOpts) ->
    #{
        token => Token,
        seq_id => 0,
        subopts => SubOpts
    }.

-spec res_changed(res()) -> res().
res_changed(#{seq_id := SeqId} = Res) ->
    NewSeqId = SeqId + 1,
    NewSeqId2 =
        case NewSeqId > ?MAX_SEQ_ID of
            true ->
                1;
            _ ->
                NewSeqId
        end,
    Res#{seq_id := NewSeqId2}.
