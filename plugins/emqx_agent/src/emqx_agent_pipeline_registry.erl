%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Registry for pipeline definitions and session profiles.
%%
%% Pipeline definitions are keyed by pipeline_id (binary).  The
%% match_trigger/1 function returns all definitions whose trigger.topic
%% MQTT filter matches the given concrete topic.
%%
%% Session profiles provide the LLM connection parameters (api_key,
%% base_url, model, instructions, output_schema) shared across multiple
%% llm_loop steps.  Profiles are optional: steps can embed session_config
%% directly instead.

-module(emqx_agent_pipeline_registry).

-behaviour(gen_server).

-export([start_link/0]).
-export([register/1, unregister/1, lookup/1, list/0, match_trigger/1]).
-export([register_profile/2, unregister_profile/1, lookup_profile/1, list_profiles/0]).
-export([delete_all/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(DEFS_TAB, emqx_agent_pipeline_defs).
-define(PROFILES_TAB, emqx_agent_pipeline_profiles).

%%--------------------------------------------------------------------
%% Public API — pipeline definitions
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec register(map()) -> ok | {error, missing_pipeline_id}.
register(#{<<"pipeline_id">> := _} = Def) ->
    gen_server:call(?MODULE, {register, Def});
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

%%--------------------------------------------------------------------
%% Public API — session profiles
%%--------------------------------------------------------------------

-spec register_profile(binary(), map()) -> ok.
register_profile(Name, Config) ->
    gen_server:call(?MODULE, {register_profile, Name, Config}).

-spec unregister_profile(binary()) -> ok.
unregister_profile(Name) ->
    gen_server:call(?MODULE, {unregister_profile, Name}).

-spec lookup_profile(binary()) -> {ok, map()} | {error, not_found}.
lookup_profile(Name) ->
    case ets:lookup(?PROFILES_TAB, Name) of
        [{_, Config}] -> {ok, Config};
        [] -> {error, not_found}
    end.

-spec list_profiles() -> [map()].
list_profiles() ->
    [Config || {_, Config} <- ets:tab2list(?PROFILES_TAB)].

-spec delete_all() -> ok.
delete_all() ->
    true = ets:delete_all_objects(?DEFS_TAB),
    true = ets:delete_all_objects(?PROFILES_TAB),
    ok.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    _ = ets:new(?DEFS_TAB, [named_table, set, public, {read_concurrency, true}]),
    _ = ets:new(?PROFILES_TAB, [named_table, set, public, {read_concurrency, true}]),
    {ok, #{}}.

handle_call({register, #{<<"pipeline_id">> := PipelineId} = Def}, _From, State) ->
    true = ets:insert(?DEFS_TAB, {PipelineId, Def}),
    {reply, ok, State};
handle_call({unregister, PipelineId}, _From, State) ->
    true = ets:delete(?DEFS_TAB, PipelineId),
    {reply, ok, State};
handle_call({register_profile, Name, Config}, _From, State) ->
    true = ets:insert(?PROFILES_TAB, {Name, Config}),
    {reply, ok, State};
handle_call({unregister_profile, Name}, _From, State) ->
    true = ets:delete(?PROFILES_TAB, Name),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
