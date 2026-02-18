%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_uns_gate_store).

-behaviour(gen_server).

-export([
    start_link/0,
    reset/0,
    active_model/0,
    list_models/0,
    get_model/1,
    put_model/2,
    activate/1,
    validate_topic/1,
    validate_message/2
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

reset() ->
    gen_server:call(?MODULE, reset).

active_model() ->
    gen_server:call(?MODULE, active_model).

list_models() ->
    gen_server:call(?MODULE, list_models).

get_model(Id) ->
    gen_server:call(?MODULE, {get_model, to_bin(Id)}).

put_model(Model, Activate) ->
    gen_server:call(?MODULE, {put_model, Model, Activate}).

activate(Id) ->
    gen_server:call(?MODULE, {activate, to_bin(Id)}).

validate_topic(Topic) ->
    gen_server:call(?MODULE, {validate_topic, Topic}).

validate_message(Topic, Payload) ->
    gen_server:call(?MODULE, {validate_message, Topic, Payload}).

init([]) ->
    {ok, #{
        active_id => undefined,
        models => #{}
    }}.

handle_call(reset, _From, _State) ->
    {reply, ok, #{active_id => undefined, models => #{}}};
handle_call(active_model, _From, State) ->
    ActiveId = maps:get(active_id, State),
    Models = maps:get(models, State),
    case maps:find(ActiveId, Models) of
        {ok, Entry} ->
            {reply, {ok, format_entry(Entry, true)}, State};
        error ->
            {reply, {error, not_found}, State}
    end;
handle_call(list_models, _From, State) ->
    ActiveId = maps:get(active_id, State),
    Models = maps:get(models, State),
    Entries = [
        format_entry(Entry, Id =:= ActiveId)
     || {Id, Entry} <- maps:to_list(Models)
    ],
    Sorted = lists:sort(
        fun(#{updated_at_ms := A}, #{updated_at_ms := B}) -> A >= B end,
        Entries
    ),
    {reply, {ok, Sorted}, State};
handle_call({get_model, Id}, _From, State) ->
    ActiveId = maps:get(active_id, State),
    Models = maps:get(models, State),
    case maps:find(Id, Models) of
        {ok, Entry} ->
            {reply, {ok, format_entry(Entry, Id =:= ActiveId)}, State};
        error ->
            {reply, {error, not_found}, State}
    end;
handle_call({put_model, Model, Activate}, _From, State) ->
    case emqx_uns_gate_model:compile(Model) of
        {ok, Compiled} ->
            Ts = erlang:system_time(millisecond),
            Id = maps:get(id, Compiled),
            Entry = #{
                id => Id,
                model => maps:get(raw, Compiled),
                compiled => Compiled,
                summary => emqx_uns_gate_model:summary(Compiled),
                updated_at_ms => Ts
            },
            Models1 = (maps:get(models, State))#{Id => Entry},
            ActiveId1 =
                case Activate of
                    true -> Id;
                    false -> maps:get(active_id, State)
                end,
            State1 = State#{models => Models1, active_id => ActiveId1},
            {reply, {ok, format_entry(Entry, ActiveId1 =:= Id)}, State1};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call({activate, Id}, _From, State) ->
    case maps:is_key(Id, maps:get(models, State)) of
        true ->
            {reply, ok, State#{active_id => Id}};
        false ->
            {reply, {error, not_found}, State}
    end;
handle_call({validate_topic, Topic}, _From, State) ->
    AllowIntermediate = emqx_uns_gate_config:allow_intermediate_publish(),
    Reply =
        case active_compiled(State) of
            undefined ->
                allow;
            Compiled ->
                emqx_uns_gate_model:validate_topic(Compiled, Topic, AllowIntermediate)
        end,
    {reply, Reply, State};
handle_call({validate_message, Topic, Payload}, _From, State) ->
    AllowIntermediate = emqx_uns_gate_config:allow_intermediate_publish(),
    ValidatePayload = emqx_uns_gate_config:validate_payload(),
    Reply =
        case active_compiled(State) of
            undefined ->
                allow;
            Compiled ->
                emqx_uns_gate_model:validate_message(
                    Compiled, Topic, Payload, AllowIntermediate, ValidatePayload
                )
        end,
    {reply, Reply, State};
handle_call(_Call, _From, State) ->
    {reply, {error, bad_request}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

active_compiled(State) ->
    ActiveId = maps:get(active_id, State),
    case maps:find(ActiveId, maps:get(models, State)) of
        {ok, Entry} -> maps:get(compiled, Entry);
        error -> undefined
    end.

format_entry(Entry, Active) ->
    #{
        id => maps:get(id, Entry),
        active => Active,
        updated_at_ms => maps:get(updated_at_ms, Entry),
        summary => maps:get(summary, Entry),
        model => maps:get(model, Entry)
    }.

to_bin(V) when is_binary(V) -> V;
to_bin(V) when is_atom(V) -> atom_to_binary(V, utf8);
to_bin(V) when is_list(V) -> unicode:characters_to_binary(V).
