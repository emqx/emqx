%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Registry for pipeline definitions.
%%
%% Pipeline definitions are keyed by pipeline_id (binary).  The
%% match_trigger/1 function returns all definitions whose trigger.topic
%% MQTT filter matches the given concrete topic.
%%
-module(emqx_agent_pipeline_registry).

-behaviour(gen_server).

-export([start_link/0]).
-export([register/1, unregister/1, lookup/1, list/0, match_trigger/1]).
-export([delete_all/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(DEFS_TAB, emqx_agent_pipeline_defs).

%%--------------------------------------------------------------------
%% Public API — pipeline definitions
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec register(map()) -> ok | {error, term()}.
register(#{<<"pipeline_id">> := _} = Def) ->
    case normalize_pipeline(Def) of
        {ok, Normalized} -> gen_server:call(?MODULE, {register, Normalized});
        {error, _} = Error -> Error
    end;
register(_) ->
    {error, missing_pipeline_id}.

-spec unregister(binary()) -> ok.
unregister(PipelineId) ->
    gen_server:call(?MODULE, {unregister, PipelineId}).

-spec lookup(binary()) -> {ok, map()} | {error, not_found}.
lookup(PipelineId) ->
    case ets:lookup(?DEFS_TAB, PipelineId) of
        [{_, Def}] -> {ok, Def};
        [] -> {error, not_found}
    end.

-spec list() -> [map()].
list() ->
    [Def || {_, Def} <- ets:tab2list(?DEFS_TAB)].

%% Return all registered pipeline definitions whose trigger.topic MQTT
%% filter matches the given concrete topic string.
-spec match_trigger(binary()) -> [map()].
match_trigger(Topic) ->
    ets:foldl(
        fun({_, Def}, Acc) ->
            TrigTopic = maps:get(
                <<"topic">>,
                maps:get(<<"trigger">>, Def, #{}),
                undefined
            ),
            case TrigTopic of
                undefined ->
                    Acc;
                Filter ->
                    case emqx_topic:match(Topic, Filter) of
                        true -> [Def | Acc];
                        false -> Acc
                    end
            end
        end,
        [],
        ?DEFS_TAB
    ).

-spec delete_all() -> ok.
delete_all() ->
    true = ets:delete_all_objects(?DEFS_TAB),
    ok.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    _ = ets:new(?DEFS_TAB, [named_table, set, public, {read_concurrency, true}]),
    {ok, #{}}.

handle_call({register, #{<<"pipeline_id">> := PipelineId} = Def}, _From, State) ->
    true = ets:insert(?DEFS_TAB, {PipelineId, Def}),
    {reply, ok, State};
handle_call({unregister, PipelineId}, _From, State) ->
    true = ets:delete(?DEFS_TAB, PipelineId),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Validation and normalization
%%--------------------------------------------------------------------

normalize_pipeline(#{<<"steps">> := Steps} = Def) when is_list(Steps) ->
    case normalize_steps(Steps, 1, []) of
        {ok, NormalizedSteps} -> {ok, Def#{<<"steps">> => NormalizedSteps}};
        {error, _} = Error -> Error
    end;
normalize_pipeline(#{}) ->
    {error, {missing_field, <<"steps">>}}.

normalize_steps([], _Index, Acc) ->
    {ok, lists:reverse(Acc)};
normalize_steps([Step | Rest], Index, Acc) when is_map(Step) ->
    case normalize_step(Step, Index) of
        {ok, Normalized} -> normalize_steps(Rest, Index + 1, [Normalized | Acc]);
        {error, _} = Error -> Error
    end;
normalize_steps([_ | _], Index, _Acc) ->
    {error, {invalid_step, Index}}.

normalize_step(#{<<"type">> := <<"llm_loop">>} = Step0, Index) ->
    Defaults = #{
        <<"tools">> => [],
        <<"input">> => #{},
        <<"stop_on_finish">> => true,
        <<"max_tokens">> => 2048
    },
    Step = maps:merge(Defaults, Step0),
    Required = [
        <<"id">>,
        <<"provider_name">>,
        <<"model">>,
        <<"instructions">>,
        <<"result_path">>,
        <<"set_result_schema">>
    ],
    case missing_required_field(Step, Required) of
        undefined -> {ok, Step};
        Field -> {error, {missing_step_field, Index, Field}}
    end;
normalize_step(Step, _Index) ->
    {ok, Step}.

missing_required_field(_Step, []) ->
    undefined;
missing_required_field(Step, [Field | Rest]) ->
    case maps:is_key(Field, Step) of
        true -> missing_required_field(Step, Rest);
        false -> Field
    end.
