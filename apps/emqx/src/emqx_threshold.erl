%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_threshold).

-dialyzer({no_improper_lists, [run/4, trigger/2, cons/2]}).

-export([
    init/1,
    run/3,
    account/3,
    trigger/1,
    reset/2
]).

-export_type([state/0]).

-type name() :: atom().
-type metric() :: count | bytes.
-type threshold() :: pos_integer().

-type trigger() :: {name(), metric(), threshold()}.
-type result() :: state() | maybe_improper_list(name(), state()).

-opaque state() :: tuple().

-define(M_COUNT, 0).
-define(M_BYTES, 1).

-doc "Initialize threshold trigger state.".
-spec init([trigger()]) ->
    state().
init(Triggers) ->
    erlang:list_to_tuple([
        {Name, enc_metric(Metric), enc_value(Threshold) bor enc_metric(Metric)}
     || {Name, Metric, Threshold} <- Triggers
    ]).

-spec run(_Count :: non_neg_integer(), _Bytes :: non_neg_integer(), state()) ->
    result().
run(Count, Bytes, State) ->
    run(Count, Bytes, State, tuple_size(State)).

run(_Count, _Bytes, State, 0) ->
    State;
run(Count, Bytes, State0, I) ->
    {Name, EV0, EThreshold} = element(I, State0),
    EMetric = EV0 band 1,
    EV1 =
        case EMetric of
            ?M_COUNT -> EV0 + enc_value(Count);
            ?M_BYTES -> EV0 + enc_value(Bytes)
        end,
    case EV1 >= EThreshold of
        false ->
            State = setelement(I, State0, {Name, EV1, EThreshold}),
            run(Count, Bytes, State, I - 1);
        true ->
            cons(Name, run(Count, Bytes, reset(Name, State0), I - 1))
    end.

-spec account(_Count :: non_neg_integer(), _Bytes :: non_neg_integer(), state()) ->
    state().
account(Count, Bytes, State) ->
    account(Count, Bytes, State, tuple_size(State)).

account(_Count, _Bytes, State, 0) ->
    State;
account(Count, Bytes, State0, I) ->
    {Name, EV0, EThreshold} = element(I, State0),
    EMetric = EV0 band 1,
    EV1 =
        case EMetric of
            ?M_COUNT -> EV0 + enc_value(Count);
            ?M_BYTES -> EV0 + enc_value(Bytes)
        end,
    State = setelement(I, State0, {Name, EV1, EThreshold}),
    account(Count, Bytes, State, I - 1).

-spec trigger(state()) ->
    result().
trigger(State) ->
    trigger(State, tuple_size(State)).

trigger(State, 0) ->
    State;
trigger(State0, I) ->
    {Name, EV0, EThreshold} = element(I, State0),
    case EV0 >= EThreshold of
        false ->
            trigger(State0, I - 1);
        true ->
            cons(Name, trigger(reset(Name, State0), I - 1))
    end.

-spec reset(name(), state()) ->
    state().
reset(Name, State) ->
    reset(Name, State, tuple_size(State)).

reset(_Name, State, 0) ->
    State;
reset(Name, State0, I) ->
    case element(I, State0) of
        {Name, EV, EThreshold} ->
            State = setelement(I, State0, {Name, EV band 1, EThreshold}),
            reset(Name, State, I - 1);
        _ ->
            reset(Name, State0, I - 1)
    end.

-spec cons(name(), result()) -> result().
cons(Name, Result) ->
    [Name | Result].

-compile({inline, [enc_metric/1]}).
enc_metric(count) -> ?M_COUNT;
enc_metric(bytes) -> ?M_BYTES.

-compile({inline, [enc_value/1]}).
enc_value(I) ->
    I bsl 1.
