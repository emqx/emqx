%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Registry for limiter groups.
%% NOTE
%% This module is not designed to be used outside of the `emqx_limiter` application.

-module(emqx_limiter_registry).

-include_lib("emqx/include/logger.hrl").

-export([
    start_link/0,
    register_group/3,
    register_group/4,
    unregister_group/1,
    find_group/1,
    list_groups/0,
    get_limiter_options/1,
    get_limiter/1
]).

%% `gen_server` callbacks

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

%% `buckets` holds module-specific shared state per limiter (e.g. the
%% shared limiter's atomics refs). It lives in the same persistent_term
%% record as the options so that one read yields both, and so that
%% consumers copy nothing onto their heap.
-record(group, {
    name :: group(),
    module :: module(),
    limiter_options :: #{name() => emqx_limiter:options()},
    buckets = #{} :: #{name() => term()}
}).

%%--------------------------------------------------------------------
%% gen_server messages
%%--------------------------------------------------------------------

-record(register_group, {
    group :: group(),
    module :: module(),
    limiter_options :: #{name() => emqx_limiter:options()},
    %% `keep` carries the existing buckets over (option update).
    buckets :: keep | #{name() => term()}
}).

-record(unregister_group, {
    group :: group()
}).

-record(list_groups, {}).

%%------------------------------------------------------------------------------
%% Internal API
%%------------------------------------------------------------------------------

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Re-register a group with new options, keeping its buckets.
-spec register_group(group(), module(), [{name(), emqx_limiter:options()}]) -> ok | no_return().
register_group(Group, Module, LimiterOptions) ->
    register_group(Group, Module, LimiterOptions, keep).

%% Register a group together with its buckets in one write. Creating a
%% new key does not trigger the persistent_term global GC; replacing one
%% does, so creation must not write twice.
-spec register_group(
    group(), module(), [{name(), emqx_limiter:options()}], keep | #{name() => term()}
) -> ok | no_return().
register_group(Group, Module, LimiterOptions, Buckets) ->
    ok = assert_unique_names(LimiterOptions),
    case
        gen_server:call(
            ?MODULE,
            #register_group{
                group = Group,
                module = Module,
                limiter_options = maps:from_list(LimiterOptions),
                buckets = Buckets
            },
            infinity
        )
    of
        ok ->
            ok;
        {error, Reason} ->
            error(Reason)
    end.

-spec unregister_group(group()) -> ok.
unregister_group(Group) ->
    gen_server:call(?MODULE, #unregister_group{group = Group}, infinity).

-spec list_groups() -> [group()].
list_groups() ->
    gen_server:call(?MODULE, #list_groups{}, infinity).

-spec find_group(group()) -> {module(), [{name(), emqx_limiter:options()}]} | undefined.
find_group(Group) ->
    case persistent_term:get(?PT_KEY(Group), undefined) of
        undefined ->
            undefined;
        #group{module = Module, limiter_options = LimiterOptions} ->
            {Module, maps:to_list(LimiterOptions)}
    end.

-spec get_limiter_options(limiter_id()) -> emqx_limiter:options() | no_return().
get_limiter_options({Group, Name} = LimiterId) ->
    case persistent_term:get(?PT_KEY(Group), undefined) of
        #group{limiter_options = #{Name := LimiterOptions}} ->
            LimiterOptions;
        _ ->
            error({limiter_not_found, LimiterId})
    end.

%% Options and bucket of one limiter from a single persistent_term read.
%% The bucket is `undefined` when the group has stored none.
-spec get_limiter(limiter_id()) -> {emqx_limiter:options(), _Bucket :: term()} | no_return().
get_limiter({Group, Name} = LimiterId) ->
    case persistent_term:get(?PT_KEY(Group), undefined) of
        #group{limiter_options = #{Name := LimiterOptions}, buckets = Buckets} ->
            {LimiterOptions, maps:get(Name, Buckets, undefined)};
        _ ->
            error({limiter_not_found, LimiterId})
    end.

buckets(#register_group{buckets = keep}, #group{buckets = Buckets}) ->
    Buckets;
buckets(#register_group{buckets = keep}, undefined) ->
    #{};
buckets(#register_group{buckets = Buckets}, _OldGroup) ->
    Buckets.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
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
                name = Group,
                module = Module,
                limiter_options = LimiterOptions,
                buckets = buckets(Req, OldGroup)
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
