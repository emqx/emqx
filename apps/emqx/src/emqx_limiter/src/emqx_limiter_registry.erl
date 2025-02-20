%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_limiter_registry).

-include_lib("emqx/include/logger.hrl").

-export([
    start_link/0,
    register_group/3,
    unregister_group/1,
    find_group/1,
    list_groups/0,
    connect/1,
    get_limiter_options/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-type group() :: emqx_limiter:group().
-type name() :: emqx_limiter:name().
-type limiter_id() :: emqx_limiter:id().

-define(PT_KEY(GROUP), {?MODULE, GROUP}).

-record(group, {
    name :: group(),
    module :: module(),
    limiter_options :: #{name() => emqx_limiter:options()}
}).

%%--------------------------------------------------------------------
%% gen_server messages
%%--------------------------------------------------------------------

-record(register_group, {
    group :: group(),
    module :: module(),
    limiter_options :: #{name() => emqx_limiter:options()}
}).

-record(unregister_group, {
    group :: group()
}).

-record(list_groups, {}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec register_group(group(), module(), [{name(), emqx_limiter:options()}]) -> ok | no_return().
register_group(Group, Module, LimiterOptions) ->
    ok = assert_unique_names(LimiterOptions),
    case
        gen_server:call(?MODULE, #register_group{
            group = Group, module = Module, limiter_options = maps:from_list(LimiterOptions)
        })
    of
        ok ->
            ok;
        {error, Reason} ->
            error(Reason)
    end.

-spec unregister_group(group()) -> ok.
unregister_group(Group) ->
    gen_server:call(?MODULE, #unregister_group{group = Group}).

-spec list_groups() -> [group()].
list_groups() ->
    gen_server:call(?MODULE, #list_groups{}).

-spec find_group(group()) -> {module(), [{name(), emqx_limiter:options()}]} | undefined.
find_group(Group) ->
    case persistent_term:get(?PT_KEY(Group), undefined) of
        undefined ->
            undefined;
        #group{module = Module, limiter_options = LimiterOptions} ->
            {Module, maps:to_list(LimiterOptions)}
    end.

-spec connect(limiter_id()) -> emqx_limiter_client:t().
connect({Group, _Name} = LimiterId) ->
    case persistent_term:get(?PT_KEY(Group), undefined) of
        undefined ->
            error({limiter_not_found, LimiterId});
        #group{module = Module} ->
            Module:connect(LimiterId)
    end.

-spec get_limiter_options(limiter_id()) -> emqx_limiter:options() | no_return().
get_limiter_options({Group, Name} = LimiterId) ->
    case persistent_term:get(?PT_KEY(Group), undefined) of
        #group{limiter_options = #{Name := LimiterOptions}} ->
            LimiterOptions;
        _ ->
            error({limiter_not_found, LimiterId})
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    {ok, #{
        group_names => sets:new([{version, 2}])
    }}.

handle_call(
    #register_group{group = Group, module = Module, limiter_options = LimiterOptions} = Req,
    _From,
    #{group_names := GroupNames} = State
) ->
    OldGroup = persistent_term:get(?PT_KEY(Group), undefined),
    case ensure_same_limiters(OldGroup, Req) of
        ok ->
            _ = persistent_term:put(?PT_KEY(Group), #group{
                name = Group, module = Module, limiter_options = LimiterOptions
            }),
            {reply, ok, State#{
                group_names := sets:add_element(Group, GroupNames)
            }};
        {error, _} = Error ->
            {reply, Error, State}
    end;
handle_call(#unregister_group{group = Group}, _From, #{group_names := GroupNames} = State) ->
    _ = persistent_term:erase(?PT_KEY(Group)),
    {reply, ok, State#{
        group_names := sets:del_element(Group, GroupNames)
    }};
handle_call(#list_groups{}, _From, #{group_names := GroupNames} = State) ->
    {reply, sets:to_list(GroupNames), State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignore, State}.

handle_cast(Req, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Req}),
    {noreply, State}.

handle_info(Req, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Req}),
    {noreply, State}.

terminate(_Reason, #{group_names := GroupNames} = _State) ->
    lists:foreach(
        fun(GroupName) ->
            _ = persistent_term:erase(?PT_KEY(GroupName))
        end,
        sets:to_list(GroupNames)
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

ensure_same_limiters(undefined = _OldGroup, _Req) ->
    ok;
ensure_same_limiters(#group{module = OldModule}, #register_group{module = NewModule}) when
    OldModule =/= NewModule
->
    {error, {different_limiter_modules, {old, OldModule}, {new, NewModule}}};
ensure_same_limiters(#group{limiter_options = OldLimiterOptions}, #register_group{
    limiter_options = NewLimiterOptions
}) ->
    OldLimiterNames = lists:sort(maps:keys(OldLimiterOptions)),
    NewLimiterNames = lists:sort(maps:keys(NewLimiterOptions)),
    case OldLimiterNames =:= NewLimiterNames of
        true ->
            ok;
        false ->
            {error, {different_limiter_names, {old, OldLimiterNames}, {new, NewLimiterNames}}}
    end.

assert_unique_names(LimiterOptions) ->
    {Names, _} = lists:unzip(LimiterOptions),
    case Names -- lists:usort(Names) of
        [] ->
            ok;
        Duplicates ->
            error({duplicate_names, Duplicates})
    end.
