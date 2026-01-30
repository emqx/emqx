%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    subscriptions/1,
    current_value/0,
    observe_value/1
]).
-export_type([manager/0]).

-define(MAX_SEQ_ID, 16777215).
-define(OBSERVE_TICK_US, 31).

-type token() :: binary().
-type seq_id() :: non_neg_integer().

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

-spec current_value() -> non_neg_integer().
-spec observe_value(seq_id()) -> non_neg_integer().

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
-spec new_res(token(), emqx_types:subopts()) -> res().
new_res(Token, SubOpts) ->
    #{
        token => Token,
        %% RFC 7641 Section 4.4: start Observe sequence at a current value.
        seq_id => current_seq(),
        subopts => SubOpts
    }.

-spec res_changed(res()) -> res().
res_changed(#{seq_id := SeqId} = Res) ->
    %% RFC 7641 Section 4.4: sequence numbers are strictly increasing.
    Now = current_seq(),
    Res#{seq_id := max(Now, SeqId + 1)}.

%% RFC 7641 Section 4.4: derive Observe value from a local timestamp.
current_value() ->
    observe_value(current_seq()).

observe_value(SeqId) ->
    SeqId band ?MAX_SEQ_ID.

current_seq() ->
    erlang:monotonic_time(microsecond) div ?OBSERVE_TICK_US.
