%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_skill_registry).

-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([register/1, unregister/1, lookup/1, list/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(TAB, ?MODULE).

-type skill_id() :: binary().

-type skill() :: #{
    skill_id := skill_id(),
    type := binary(),
    version := binary(),
    display_name := binary(),
    description := binary(),
    context => term(),
    input_schema => map(),
    output_schema => map()
}.

-export_type([skill/0, skill_id/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec register(skill()) -> ok | {error, missing_skill_id}.
register(#{skill_id := _} = Skill) ->
    gen_server:call(?MODULE, {register, Skill});
register(_Skill) ->
    {error, missing_skill_id}.

-spec unregister(skill_id()) -> ok.
unregister(SkillId) ->
    gen_server:call(?MODULE, {unregister, SkillId}).

-spec lookup(skill_id()) -> {ok, skill()} | {error, not_found}.
lookup(SkillId) ->
    case ets:lookup(?TAB, SkillId) of
        [{_Id, Skill}] -> {ok, Skill};
        [] -> {error, not_found}
    end.

-spec list() -> [skill()].
list() ->
    [Skill || {_Id, Skill} <- ets:tab2list(?TAB)].

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    _ = ets:new(?TAB, [named_table, set, public, {read_concurrency, true}]),
    {ok, #{}}.

handle_call({register, #{skill_id := SkillId} = Skill}, _From, State) ->
    true = ets:insert(?TAB, {SkillId, Skill}),
    {reply, ok, State};
handle_call({unregister, SkillId}, _From, State) ->
    true = ets:delete(?TAB, SkillId),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
