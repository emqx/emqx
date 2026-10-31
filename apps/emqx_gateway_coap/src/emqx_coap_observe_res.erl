%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_observe_res).

-include("emqx_coap.hrl").

%% API
-export([
    new_manager/0,
    insert/2,
    update_subscription/2,
    remove/2,
    remove/3,
    lookup/3,
    tokens/2,
    res_changed/2,
    resource_context/2,
    foreach/2,
    subscriptions/1,
    current_value/0,
    next_value/0,
    confirmation_interval/0,
    observe_value/1
]).
-export_type([manager/0]).

-define(MAX_SEQ_ID, 16777215).
-define(OBSERVE_TICK_US, 31).

-type token() :: binary().
-type seq_id() :: non_neg_integer().

-type res() :: #{
    id := reference(),
    token := token(),
    clientid => emqx_types:clientid() | undefined,
    seq_id := seq_id(),
    subopts := emqx_types:subopts(),
    resource_context => resource_context() | undefined
}.

-type manager() :: #{emqx_types:topic() => #{token() => res()}}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
-spec new_manager() -> manager().
new_manager() ->
    #{}.

-spec insert(sub_data(), manager()) -> {seq_id(), manager()}.
insert(#{topic := Topic, token := Token, subopts := SubOpts} = Sub, Manager) ->
    Observers = maps:get(Topic, Manager, #{}),
    Context = maps:get(resource_context, Sub, undefined),
    Res =
        case maps:get(Token, Observers, undefined) of
            undefined ->
                new_res(Token, SubOpts);
            #{resource_context := Context} = Existing ->
                Existing#{subopts := SubOpts};
            _ ->
                new_res(Token, SubOpts)
        end,
    Res1 = Res#{
        clientid => maps:get(clientid, Sub, undefined),
        resource_context => maps:get(resource_context, Sub, undefined)
    },
    {maps:get(seq_id, Res1), Manager#{Topic => Observers#{Token => Res1}}}.

-spec update_subscription(sub_data(), manager()) -> manager().
update_subscription(#{topic := Topic, subopts := SubOpts} = SubData, Manager) ->
    case maps:find(Topic, Manager) of
        {ok, Observers} ->
            Updated = maps:map(fun(_Token, Res) -> Res#{subopts := SubOpts} end, Observers),
            Manager#{Topic := Updated};
        error ->
            {_SeqId, Manager1} = insert(SubData, Manager),
            Manager1
    end.

-spec remove(emqx_types:topic(), manager()) -> manager().
remove(Topic, Manager) ->
    maps:remove(Topic, Manager).

-spec remove(emqx_types:topic(), token(), manager()) -> manager().
remove(Topic, Token, Manager) ->
    Observers = maps:remove(Token, maps:get(Topic, Manager, #{})),
    case map_size(Observers) of
        0 -> maps:remove(Topic, Manager);
        _ -> Manager#{Topic => Observers}
    end.

-spec lookup(emqx_types:topic(), token(), manager()) -> res() | undefined.
lookup(Topic, Token, Manager) ->
    maps:get(Token, maps:get(Topic, Manager, #{}), undefined).

-spec tokens(emqx_types:topic(), manager()) -> [token()].
tokens(Topic, Manager) ->
    maps:keys(maps:get(Topic, Manager, #{})).

-spec res_changed(emqx_types:topic(), manager()) -> undefined | {[{token(), seq_id()}], manager()}.
res_changed(Topic, Manager) ->
    case maps:get(Topic, Manager, undefined) of
        undefined ->
            undefined;
        Observers ->
            Updated = maps:map(fun(_Token, Res) -> res_changed(Res) end, Observers),
            Notifications = [
                {Token, SeqId}
             || {Token, #{seq_id := SeqId}} <- maps:to_list(Updated)
            ],
            {Notifications, Manager#{Topic := Updated}}
    end.

-spec resource_context(token(), manager()) -> resource_context() | undefined.
resource_context(Token, Manager) ->
    Contexts = [
        Context
     || Observers <- maps:values(Manager),
        #{token := T, resource_context := Context} <- maps:values(Observers),
        T =:= Token
    ],
    case Contexts of
        [Context] -> Context;
        _ -> undefined
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
        fun(_Topic, Observers) ->
            Opts = [SubOpts || #{subopts := SubOpts} <- maps:values(Observers)],
            [First | _] = Opts,
            First#{
                qos => lists:max([maps:get(qos, O, 0) || O <- Opts]),
                nl => lists:min([maps:get(nl, O, 0) || O <- Opts])
            }
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
        id => make_ref(),
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

-spec next_value() -> non_neg_integer().
next_value() ->
    Now = current_seq(),
    SeqId =
        case get({?MODULE, seq_id}) of
            undefined -> Now;
            Last -> max(Now, Last + 1)
        end,
    put({?MODULE, seq_id}, SeqId),
    observe_value(SeqId).

-spec confirmation_interval() -> pos_integer().
confirmation_interval() ->
    timer:hours(24).

observe_value(SeqId) ->
    SeqId band ?MAX_SEQ_ID.

current_seq() ->
    erlang:monotonic_time(microsecond) div ?OBSERVE_TICK_US.
