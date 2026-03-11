%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_skill_registry).

-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([register/1, unregister/2, lookup/2, list/0, list/1, delete_all/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(TAB, ?MODULE).

-type skill_id() :: binary().
-type skill_type() :: binary().

-type skill() :: #{
    skill_id := skill_id(),
    type := skill_type(),
    display_name := binary(),
    description := binary(),
    context => term(),
    input_schema => map(),
    output_schema => map()
}.

-export_type([skill/0, skill_id/0, skill_type/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec register(skill()) -> ok | {error, missing_skill_id | missing_type}.
register(#{skill_id := _, type := _} = Skill) ->
    gen_server:call(?MODULE, {register, Skill});
register(#{skill_id := _}) ->
    {error, missing_type};
register(_Skill) ->
    {error, missing_skill_id}.

-spec unregister(skill_type(), skill_id()) -> ok.
unregister(Type, SkillId) ->
    gen_server:call(?MODULE, {unregister, {Type, SkillId}}).

-spec lookup(skill_type(), skill_id()) -> {ok, skill()} | {error, not_found}.
lookup(Type, SkillId) ->
    case ets:lookup(?TAB, {Type, SkillId}) of
        [{_Key, Skill}] -> {ok, Skill};
        [] -> {error, not_found}
    end.

-spec list() -> [skill()].
list() ->
    [Skill || {_Key, Skill} <- ets:tab2list(?TAB)].

-spec list(skill_type()) -> [skill()].
list(Type) ->
    [Skill || {Key, Skill} <- ets:tab2list(?TAB), element(1, Key) =:= Type].

-spec delete_all() -> ok.
delete_all() ->
    true = ets:delete_all_objects(?TAB),
    ok.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    _ = ets:new(?TAB, [named_table, set, public, {read_concurrency, true}]),
    {ok, #{}}.

handle_call({register, #{skill_id := SkillId, type := Type} = Skill}, _From, State) ->
    true = ets:insert(?TAB, {{Type, SkillId}, Skill}),
    {reply, ok, State};
handle_call({unregister, Key}, _From, State) ->
    true = ets:delete(?TAB, Key),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
