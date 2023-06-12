%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mqttsn_session).

-export([registry/1, set_registry/2]).

-export([
    init/1,
    info/1,
    info/2,
    stats/1,
    resume/2
]).

-export([
    publish/4,
    subscribe/4,
    unsubscribe/4,
    puback/3,
    pubrec/3,
    pubrel/3,
    pubcomp/3
]).

-export([
    replay/2,
    deliver/3,
    obtain_next_pkt_id/1,
    takeover/1,
    enqueue/3,
    retry/2,
    expire/3
]).

-type session() :: #{
    registry := emqx_mqttsn_registry:registry(),
    session := emqx_session:session()
}.

-export_type([session/0]).

init(ClientInfo) ->
    Conf = emqx_cm:get_session_confs(
        ClientInfo, #{receive_maximum => 1, expiry_interval => 0}
    ),
    #{
        registry => emqx_mqttsn_registry:init(),
        session => emqx_session:init(Conf)
    }.

registry(#{registry := Registry}) ->
    Registry.

set_registry(Registry, Session) ->
    Session#{registry := Registry}.

info(#{session := Session}) ->
    emqx_session:info(Session).

info(Key, #{session := Session}) ->
    emqx_session:info(Key, Session).

stats(#{session := Session}) ->
    emqx_session:stats(Session).

puback(ClientInfo, MsgId, Session) ->
    with_sess(?FUNCTION_NAME, [ClientInfo, MsgId], Session).

pubrec(ClientInfo, MsgId, Session) ->
    with_sess(?FUNCTION_NAME, [ClientInfo, MsgId], Session).

pubrel(ClientInfo, MsgId, Session) ->
    with_sess(?FUNCTION_NAME, [ClientInfo, MsgId], Session).

pubcomp(ClientInfo, MsgId, Session) ->
    with_sess(?FUNCTION_NAME, [ClientInfo, MsgId], Session).

publish(ClientInfo, MsgId, Msg, Session) ->
    with_sess(?FUNCTION_NAME, [ClientInfo, MsgId, Msg], Session).

subscribe(ClientInfo, Topic, SubOpts, Session) ->
    with_sess(?FUNCTION_NAME, [ClientInfo, Topic, SubOpts], Session).

unsubscribe(ClientInfo, Topic, SubOpts, Session) ->
    with_sess(?FUNCTION_NAME, [ClientInfo, Topic, SubOpts], Session).

replay(ClientInfo, Session) ->
    with_sess(?FUNCTION_NAME, [ClientInfo], Session).

deliver(ClientInfo, Delivers, Session1) ->
    with_sess(?FUNCTION_NAME, [ClientInfo, Delivers], Session1).

obtain_next_pkt_id(Session = #{session := Sess}) ->
    {Id, Sess1} = emqx_session:obtain_next_pkt_id(Sess),
    {Id, Session#{session := Sess1}}.

takeover(_Session = #{session := Sess}) ->
    emqx_session:takeover(Sess).

enqueue(ClientInfo, Delivers, Session = #{session := Sess}) ->
    Sess1 = emqx_session:enqueue(ClientInfo, Delivers, Sess),
    Session#{session := Sess1}.

retry(ClientInfo, Session) ->
    with_sess(?FUNCTION_NAME, [ClientInfo], Session).

expire(ClientInfo, awaiting_rel, Session) ->
    with_sess(?FUNCTION_NAME, [ClientInfo, awaiting_rel], Session).

resume(ClientInfo, #{session := Sess}) ->
    emqx_session:resume(ClientInfo, Sess).

%%--------------------------------------------------------------------
%% internal funcs

with_sess(Fun, Args, Session = #{session := Sess}) ->
    case apply(emqx_session, Fun, Args ++ [Sess]) of
        %% for subscribe
        {error, Reason} ->
            {error, Reason};
        %% for pubrel
        {ok, Sess1} ->
            {ok, Session#{session := Sess1}};
        %% for publish and puback
        {ok, Result, Sess1} ->
            {ok, Result, Session#{session := Sess1}};
        %% for puback
        {ok, Msgs, Replies, Sess1} ->
            {ok, Msgs, Replies, Session#{session := Sess1}}
    end.
