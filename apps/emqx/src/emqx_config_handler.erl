%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
%% The sub config handlers maintain independent parts of the emqx config map
%% And there are a top level config handler maintains the overall config map.
-module(emqx_config_handler).

-include("logger.hrl").
-include("emqx.hrl").
-include("emqx_schema.hrl").
-include_lib("hocon/include/hocon_types.hrl").

-behaviour(gen_server).

%% API functions
-export([
    start_link/0,
    stop/0,
    add_handler/2,
    remove_handler/1,
    update_config/3,
    update_config/4,
    get_raw_cluster_override_conf/0,
    info/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export([schema/2]).

-define(MOD, '$mod').
-define(WKEY, '?').

-type handler_name() :: module().

-optional_callbacks([
    pre_config_update/3,
    pre_config_update/4,
    propagated_pre_config_update/3,
    propagated_pre_config_update/4,
    post_config_update/5,
    post_config_update/6,
    propagated_post_config_update/5,
    propagated_post_config_update/6
]).

-callback pre_config_update([atom()], emqx_config:update_request(), emqx_config:raw_config()) ->
    ok | {ok, emqx_config:update_request()} | {error, term()}.
-callback propagated_pre_config_update(
    [binary()], emqx_config:update_request(), emqx_config:raw_config()
) ->
    ok | {ok, emqx_config:update_request()} | {error, term()}.

-callback post_config_update(
    [atom()],
    emqx_config:update_request(),
    emqx_config:config(),
    emqx_config:config(),
    emqx_config:app_envs()
) ->
    ok | {ok, Result :: any()} | {error, Reason :: term()}.

-callback propagated_post_config_update(
    [atom()],
    emqx_config:update_request(),
    emqx_config:config(),
    emqx_config:config(),
    emqx_config:app_envs()
) ->
    ok | {ok, Result :: any()} | {error, Reason :: term()}.

-callback pre_config_update(
    [atom()], emqx_config:update_request(), emqx_config:raw_config(), emqx_config:cluster_rpc_opts()
) ->
    ok | {ok, emqx_config:update_request()} | {error, term()}.
-callback propagated_pre_config_update(
    [binary()],
    emqx_config:update_request(),
    emqx_config:raw_config(),
    emqx_config:cluster_rpc_opts()
) ->
    ok | {ok, emqx_config:update_request()} | {error, term()}.

-callback post_config_update(
    [atom()],
    emqx_config:update_request(),
    emqx_config:config(),
    emqx_config:config(),
    emqx_config:app_envs(),
    emqx_config:cluster_rpc_opts()
) ->
    ok | {ok, Result :: any()} | {error, Reason :: term()}.

-callback propagated_post_config_update(
    [atom()],
    emqx_config:update_request(),
    emqx_config:config(),
    emqx_config:config(),
    emqx_config:app_envs(),
    emqx_config:cluster_rpc_opts()
) ->
    ok | {ok, Result :: any()} | {error, Reason :: term()}.

-type handlers() :: #{
    ?MOD => module(),
    atom() => handlers()
}.
-type state() :: #{handlers := handlers(), _ => _}.
-type conf_key_path() :: emqx_utils_maps:config_key_path().

-record(conf_info, {
    schema_mod :: module(),
    conf_key_path :: conf_key_path(),
    update_args :: emqx_config:update_args(),
    cluster_rpc_opts :: map(),
    handlers :: handlers() | undefined
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, {}, []).

stop() ->
    gen_server:stop(?MODULE).

update_config(SchemaModule, ConfKeyPath, UpdateArgs) ->
    update_config(SchemaModule, ConfKeyPath, UpdateArgs, #{}).

-spec update_config(module(), conf_key_path(), emqx_config:update_args(), map()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
update_config(SchemaModule, ConfKeyPath, UpdateArgs, ClusterRpcOpts) ->
    %% force convert the path to a list of atoms, as there maybe some wildcard names/ids in the path
    AtomKeyPath = [atom(Key) || Key <- ConfKeyPath],
    ConfInfo = #conf_info{
        schema_mod = SchemaModule,
        conf_key_path = AtomKeyPath,
        update_args = UpdateArgs,
        cluster_rpc_opts = ClusterRpcOpts
    },
    gen_server:call(?MODULE, ConfInfo, infinity).

-spec add_handler(conf_key_path(), handler_name()) ->
    ok | {error, {conflict, list()}}.
add_handler(ConfKeyPath, HandlerName) ->
    assert_callback_function(HandlerName),
    gen_server:call(?MODULE, {add_handler, ConfKeyPath, HandlerName}).

%% @doc Remove handler asynchronously
-spec remove_handler(conf_key_path()) -> ok.
remove_handler(ConfKeyPath) ->
    gen_server:cast(?MODULE, {remove_handler, ConfKeyPath}).

get_raw_cluster_override_conf() ->
    gen_server:call(?MODULE, get_raw_cluster_override_conf).

info() ->
    gen_server:call(?MODULE, info).

%%============================================================================

-spec init(term()) -> {ok, state()}.
init(_) ->
    process_flag(trap_exit, true),
    Handlers = load_prev_handlers(),
    {ok, #{handlers => Handlers#{?MOD => ?MODULE}}}.

handle_call({add_handler, ConfKeyPath, HandlerName}, _From, State = #{handlers := Handlers}) ->
    case deep_put_handler(ConfKeyPath, Handlers, HandlerName) of
        {ok, NewHandlers} -> {reply, ok, State#{handlers => NewHandlers}};
        {error, _Reason} = Error -> {reply, Error, State}
    end;
handle_call(#conf_info{} = ConfInfo, _From, #{handlers := Handlers} = State) ->
    {reply, safe_handle_update_request(ConfInfo#conf_info{handlers = Handlers}), State};
handle_call(get_raw_cluster_override_conf, _From, State) ->
    Reply = emqx_config:read_override_conf(#{override_to => cluster}),
    {reply, Reply, State};
handle_call(info, _From, State) ->
    {reply, State, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({remove_handler, ConfKeyPath}, State = #{handlers := Handlers}) ->
    NewHandlers = do_remove_handler(ConfKeyPath, Handlers),
    {noreply, State#{handlers => NewHandlers}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%% application shutdown, we can't call application_controller here.
terminate(shutdown, _) ->
    ok;
terminate(_Reason, #{handlers := Handlers}) ->
    save_handlers(Handlers),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

deep_put_handler([], Handlers, Mod) ->
    {ok, Handlers#{?MOD => Mod}};
deep_put_handler([Key0 | KeyPath], Handlers, Mod) ->
    Key = atom(Key0),
    SubHandlers = maps:get(Key, Handlers, #{}),
    case deep_put_handler(KeyPath, SubHandlers, Mod) of
        {ok, NewSubHandlers} ->
            NewHandlers = Handlers#{Key => NewSubHandlers},
            case check_handler_conflict(NewHandlers) of
                ok -> {ok, NewHandlers};
                {error, Reason} -> {error, Reason}
            end;
        {error, _Reason} = Error ->
            Error
    end.

%% Make sure that Specify Key and ?WKEY cannot be on the same level.
%%
%% [k1, ?, ?], [k1, ?], [k1] is allow.
%% [K1, ?, k2], [k1, ?, k3] is allow.
%% [k1, ?, ?], [k1, ?, k2] is not allow.
check_handler_conflict(Handlers) ->
    Keys = filter_top_level_handlers(Handlers),
    case lists:member(?WKEY, Keys) of
        true when length(Keys) =:= 1 -> ok;
        true -> {error, {conflict, Keys}};
        false -> ok
    end.

filter_top_level_handlers(Handlers) ->
    Fun =
        fun
            (K, #{?MOD := _}, Acc) -> [K | Acc];
            (_K, #{}, Acc) -> Acc;
            (?MOD, _, Acc) -> Acc
        end,
    maps:fold(Fun, [], Handlers).

safe_handle_update_request(ConfInfo) ->
    try
        handle_update_request(ConfInfo)
    catch
        throw:Reason ->
            {error, Reason};
        Error:Reason:ST ->
            ConfInfo1 = maps:remove([handlers, update_req], conf_info_to_map(ConfInfo)),
            ?SLOG(error, #{
                msg => "update_config_failed",
                exception => Error,
                reason => Reason,
                conf_change_info => ConfInfo1,
                stacktrace => ST
            }),
            {error, {config_update_crashed, Reason}}
    end.

handle_update_request(ConfInfo) ->
    case process_update_request(ConfInfo) of
        {ok, NewRawConf, OverrideConf, Opts} ->
            check_and_save_configs(ConfInfo, NewRawConf, OverrideConf, Opts);
        {error, Result} ->
            {error, Result}
    end.

process_update_request(#conf_info{conf_key_path = [_], update_args = {remove, _Opts}}) ->
    {error, "remove_root_is_forbidden"};
process_update_request(#conf_info{conf_key_path = ConfKeyPath, update_args = {remove, Opts}}) ->
    OldRawConf = emqx_config:get_root_raw(ConfKeyPath),
    BinKeyPath = bin_path(ConfKeyPath),
    NewRawConf = emqx_utils_maps:deep_remove(BinKeyPath, OldRawConf),
    OverrideConf = remove_from_override_config(BinKeyPath, Opts),
    {ok, NewRawConf, OverrideConf, Opts};
process_update_request(#conf_info{update_args = {{update, _}, Opts}} = ConfInfo) ->
    ConfKeyPath = ConfInfo#conf_info.conf_key_path,
    OldRawConf = emqx_config:get_root_raw(ConfKeyPath),
    case do_update_config(ConfInfo, OldRawConf) of
        {ok, NewRawConf} ->
            OverrideConf = merge_to_override_config(NewRawConf, Opts),
            {ok, NewRawConf, OverrideConf, Opts};
        Error ->
            Error
    end.

do_update_config(ConfInfo, OldRawConf) ->
    ConfKeyPath = ConfInfo#conf_info.conf_key_path,
    do_update_config(ConfKeyPath, ConfInfo, OldRawConf).

do_update_config([], ConfInfo, OldRawConf) ->
    PreUpCtx = #{
        old_raw_conf => OldRawConf,
        callback => pre_config_update,
        is_propagated => false
    },
    call_pre_config_update(maps:merge(conf_info_to_map(ConfInfo), PreUpCtx));
do_update_config([ConfKey | SubConfKeyPath], ConfInfo, OldRawConf) ->
    ConfKeyBin = bin(ConfKey),
    SubOldRawConf = get_sub_config(ConfKeyBin, OldRawConf),
    SubHandlers = get_sub_handlers(ConfKey, ConfInfo#conf_info.handlers),
    ConfInfo1 = ConfInfo#conf_info{handlers = SubHandlers},
    case do_update_config(SubConfKeyPath, ConfInfo1, SubOldRawConf) of
        {ok, NewUpdateReq} ->
            merge_to_old_config(#{ConfKeyBin => NewUpdateReq}, OldRawConf);
        Error ->
            Error
    end.

check_and_save_configs(ConfInfo, NewRawConf, OverrideConf, Opts) ->
    #conf_info{
        schema_mod = SchemaModule,
        conf_key_path = ConfKeyPath,
        cluster_rpc_opts = ClusterRpcOpts
    } = ConfInfo,
    Schema = schema(SchemaModule, ConfKeyPath),
    Kind = maps:get(kind, ClusterRpcOpts, ?KIND_INITIATE),
    {AppEnvs, NewConf} = emqx_config:check_config(Schema, NewRawConf),
    OldConf = emqx_config:get_root(ConfKeyPath),
    PostUpCtx = #{old_conf => OldConf, new_conf => NewConf, app_envs => AppEnvs},
    case do_post_config_update(ConfInfo, PostUpCtx) of
        {ok, Result0} ->
            post_update_ok(ConfInfo, AppEnvs, NewConf, NewRawConf, OverrideConf, Opts, Result0);
        {error, {post_config_update, HandlerName, Reason}} when Kind =/= ?KIND_INITIATE ->
            LogMsg = "post_config_update_failed_but_save_the_config_anyway",
            ?SLOG(critical, #{msg => LogMsg, handler => HandlerName, reason => Reason}),
            post_update_ok(ConfInfo, AppEnvs, NewConf, NewRawConf, OverrideConf, Opts, #{});
        {error, _} = Error ->
            Error
    end.

post_update_ok(ConfInfo, AppEnvs, NewConf, NewRawConf, OverrideConf, Opts, Result0) ->
    ok = emqx_config:save_configs(AppEnvs, NewConf, NewRawConf, OverrideConf, Opts),
    Result1 = return_change_result(ConfInfo),
    {ok, Result1#{post_config_update => Result0}}.

do_post_config_update(ConfInfo, PostUpCtx) ->
    do_post_config_update(ConfInfo#conf_info.conf_key_path, ConfInfo, PostUpCtx).

do_post_config_update([], ConfInfo, PostUpCtx) ->
    PostUpCtx1 = maps:merge(conf_info_to_map(ConfInfo), PostUpCtx),
    call_post_config_update(PostUpCtx1#{callback => post_config_update, result => #{}});
do_post_config_update([ConfKey | SubConfKeyPath], ConfInfo, PostUpCtx) ->
    SubOldConf = get_sub_config(ConfKey, maps:get(old_conf, PostUpCtx)),
    SubNewConf = get_sub_config(ConfKey, maps:get(new_conf, PostUpCtx)),
    SubHandlers = get_sub_handlers(ConfKey, ConfInfo#conf_info.handlers),
    PostUpCtx1 = PostUpCtx#{
        old_conf := SubOldConf,
        new_conf := SubNewConf
    },
    ConfInfo1 = ConfInfo#conf_info{handlers = SubHandlers},
    do_post_config_update(SubConfKeyPath, ConfInfo1, PostUpCtx1).

get_sub_handlers(ConfKey, Handlers) when is_atom(ConfKey) ->
    case maps:find(ConfKey, Handlers) of
        error -> maps:get(?WKEY, Handlers, #{});
        {ok, SubHandlers} -> SubHandlers
    end;
get_sub_handlers(ConfKey, Handlers) when is_binary(ConfKey) ->
    ConcreteHandlerKeys = maps:keys(Handlers) -- [?MOD, ?WKEY],
    case lists:search(fun(K) -> bin(K) =:= ConfKey end, ConcreteHandlerKeys) of
        {value, Key} -> maps:get(Key, Handlers);
        false -> maps:get(?WKEY, Handlers, #{})
    end.

get_sub_config(ConfKey, Conf) when is_map(Conf) ->
    maps:get(ConfKey, Conf, undefined);
%% the Conf is a primitive
get_sub_config(_, _Conf) ->
    undefined.

call_pre_config_update(Ctx) ->
    case call_proper_pre_config_update(Ctx) of
        {ok, NewUpdateReq0} ->
            case propagate_pre_config_updates_to_subconf(Ctx#{update_req => NewUpdateReq0}) of
                {ok, #{update_req := NewUpdateReq1}} ->
                    {ok, NewUpdateReq1};
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

call_proper_pre_config_update(#{handlers := #{?MOD := Module}, callback := Callback} = Ctx) ->
    Arity = get_function_arity(Module, Callback, [3, 4]),
    case apply_pre_config_update(Module, Callback, Arity, Ctx) of
        ok ->
            {ok, maps:get(update_req, Ctx)};
        {ok, NewUpdateReq} ->
            {ok, NewUpdateReq};
        {error, Reason} ->
            {error, {pre_config_update, Module, Reason}}
    end;
call_proper_pre_config_update(#{update_req := UpdateReq}) ->
    {ok, UpdateReq}.

apply_pre_config_update(Module, Callback, 3, #{
    conf_key_path := ConfKeyPath,
    update_req := UpdateReq,
    old_raw_conf := OldRawConf
}) ->
    Module:Callback(ConfKeyPath, UpdateReq, OldRawConf);
apply_pre_config_update(Module, Callback, 4, #{
    conf_key_path := ConfKeyPath,
    update_req := UpdateReq,
    old_raw_conf := OldRawConf,
    cluster_rpc_opts := ClusterRpcOpts
}) ->
    Module:Callback(ConfKeyPath, UpdateReq, OldRawConf, ClusterRpcOpts);
apply_pre_config_update(_Module, _Callback, false, #{
    update_req := UpdateReq,
    old_raw_conf := OldRawConf
}) ->
    merge_to_old_config(UpdateReq, OldRawConf).

propagate_pre_config_updates_to_subconf(#{handlers := #{?WKEY := _}} = Ctx) ->
    #{update_req := UpdateReq, old_raw_conf := OldRawConf} = Ctx,
    Keys = propagate_keys(UpdateReq, OldRawConf),
    propagate_pre_config_updates_to_subconf_keys(Keys, Ctx);
propagate_pre_config_updates_to_subconf(#{handlers := Handlers} = Ctx) ->
    Keys = maps:keys(maps:without([?MOD], Handlers)),
    propagate_pre_config_updates_to_subconf_keys(Keys, Ctx).

propagate_pre_config_updates_to_subconf_keys([], Ctx) ->
    {ok, Ctx};
propagate_pre_config_updates_to_subconf_keys([Key | Keys], Ctx0) ->
    case do_propagate_pre_config_updates_to_subconf_key(Key, Ctx0) of
        {ok, Ctx1} ->
            propagate_pre_config_updates_to_subconf_keys(Keys, Ctx1);
        {error, _} = Error ->
            Error
    end.

do_propagate_pre_config_updates_to_subconf_key(Key, Ctx) ->
    #{
        handlers := Handlers,
        old_raw_conf := OldRawConf,
        update_req := UpdateReq,
        conf_key_path := ConfKeyPath,
        is_propagated := IsPropagated
    } = Ctx,
    BinKey = bin(Key),
    SubHandlers = get_sub_handlers(BinKey, Handlers),
    SubUpdateReq = get_sub_config(BinKey, UpdateReq),
    SubOldConf = get_sub_config(BinKey, OldRawConf),
    SubConfKeyPath =
        case IsPropagated of
            true -> ConfKeyPath ++ [BinKey];
            false -> bin_path(ConfKeyPath) ++ [BinKey]
        end,
    case {SubOldConf, SubUpdateReq} of
        %% we have handler, but no relevant keys in both configs (new and old),
        %% so we don't need to go further
        {undefined, undefined} ->
            {ok, Ctx};
        {_, _} ->
            Ctx1 = Ctx#{
                handlers := SubHandlers,
                old_raw_conf := SubOldConf,
                update_req := SubUpdateReq,
                conf_key_path := SubConfKeyPath,
                is_propagated := true,
                callback := propagated_pre_config_update
            },
            case call_pre_config_update(Ctx1) of
                {ok, SubNewConf1} ->
                    %% we update only if the new config is not to be removed
                    %% i.e. SubUpdateReq is not undefined
                    case SubUpdateReq of
                        undefined ->
                            {ok, Ctx};
                        _ ->
                            UpdateReq1 = maps:put(BinKey, SubNewConf1, UpdateReq),
                            {ok, Ctx#{update_req := UpdateReq1}}
                    end;
                {error, _} = Error ->
                    Error
            end
    end.

call_post_config_update(#{handlers := Handlers} = Ctx) ->
    case call_proper_post_config_update(Ctx) of
        {ok, Result} ->
            SubHandlers = maps:without([?MOD], Handlers),
            propagate_post_config_updates_to_subconf(Ctx#{
                handlers := SubHandlers,
                callback := propagated_post_config_update,
                result := Result
            });
        {error, _} = Error ->
            Error
    end.

call_proper_post_config_update(
    #{
        handlers := #{?MOD := Module},
        callback := Callback,
        result := Result
    } = Ctx
) ->
    Arity = get_function_arity(Module, Callback, [5, 6]),
    case apply_post_config_update(Module, Callback, Arity, Ctx) of
        ok -> {ok, Result};
        {ok, Result1} -> {ok, Result#{Module => Result1}};
        {error, Reason} -> {error, {post_config_update, Module, Reason}}
    end;
call_proper_post_config_update(
    #{result := Result} = _Ctx
) ->
    {ok, Result}.

apply_post_config_update(Module, Callback, 5, #{
    conf_key_path := ConfKeyPath,
    update_req := UpdateReq,
    new_conf := NewConf,
    old_conf := OldConf,
    app_envs := AppEnvs
}) ->
    Module:Callback(ConfKeyPath, UpdateReq, NewConf, OldConf, AppEnvs);
apply_post_config_update(Module, Callback, 6, #{
    conf_key_path := ConfKeyPath,
    update_req := UpdateReq,
    cluster_rpc_opts := ClusterRpcOpts,
    new_conf := NewConf,
    old_conf := OldConf,
    app_envs := AppEnvs
}) ->
    Module:Callback(ConfKeyPath, UpdateReq, NewConf, OldConf, AppEnvs, ClusterRpcOpts);
apply_post_config_update(_Module, _Callback, false, _Ctx) ->
    ok.

propagate_post_config_updates_to_subconf(#{handlers := #{?WKEY := _}} = Ctx) ->
    #{old_conf := OldConf, new_conf := NewConf} = Ctx,
    Keys = propagate_keys(OldConf, NewConf),
    propagate_post_config_updates_to_subconf_keys(Keys, Ctx);
propagate_post_config_updates_to_subconf(#{handlers := Handlers} = Ctx) ->
    Keys = maps:keys(Handlers),
    propagate_post_config_updates_to_subconf_keys(Keys, Ctx).

propagate_post_config_updates_to_subconf_keys([], #{result := Result}) ->
    {ok, Result};
propagate_post_config_updates_to_subconf_keys([Key | Keys], Ctx) ->
    case propagate_post_config_updates_to_subconf_key(Key, Ctx) of
        {ok, Result1} ->
            propagate_post_config_updates_to_subconf_keys(Keys, Ctx#{result := Result1});
        Error ->
            Error
    end.

propagate_keys(OldConf, NewConf) ->
    sets:to_list(sets:union(propagate_keys(OldConf), propagate_keys(NewConf))).

propagate_keys(Conf) when is_map(Conf) -> sets:from_list(maps:keys(Conf), [{version, 2}]);
propagate_keys(_) -> sets:new([{version, 2}]).

propagate_post_config_updates_to_subconf_key(Key, Ctx) ->
    #{
        handlers := Handlers,
        new_conf := NewConf,
        old_conf := OldConf,
        result := Result,
        conf_key_path := ConfKeyPath
    } = Ctx,
    SubHandlers = maps:get(Key, Handlers, maps:get(?WKEY, Handlers, undefined)),
    SubNewConf = get_sub_config(Key, NewConf),
    SubOldConf = get_sub_config(Key, OldConf),
    SubConfKeyPath = ConfKeyPath ++ [Key],
    call_post_config_update(Ctx#{
        handlers := SubHandlers,
        new_conf := SubNewConf,
        old_conf := SubOldConf,
        result := Result,
        conf_key_path := SubConfKeyPath,
        callback := propagated_post_config_update
    }).

%% The default callback of config handlers
%% the behaviour is overwriting the old config if:
%%   1. the old config is undefined
%%   2. either the old or the new config is not of map type
%% the behaviour is merging the new the config to the old config if they are maps.

merge_to_old_config(UpdateReq, RawConf) when is_map(UpdateReq), is_map(RawConf) ->
    {ok, maps:merge(RawConf, UpdateReq)};
merge_to_old_config(UpdateReq, _RawConf) ->
    {ok, UpdateReq}.

remove_from_override_config(_BinKeyPath, #{persistent := false}) ->
    undefined;
remove_from_override_config(BinKeyPath, Opts) ->
    OldConf = emqx_config:read_override_conf(Opts),
    UpgradedOldConf = upgrade_conf(OldConf),
    emqx_utils_maps:deep_remove(BinKeyPath, UpgradedOldConf).

%% apply new config on top of override config
merge_to_override_config(_RawConf, #{persistent := false}) ->
    undefined;
merge_to_override_config(RawConf, Opts) ->
    OldConf = emqx_config:read_override_conf(Opts),
    UpgradedOldConf = upgrade_conf(OldConf),
    maps:merge(UpgradedOldConf, RawConf).

upgrade_conf(Conf) ->
    ConfigLoader = emqx_app:get_config_loader(),
    %% ensure module loaded
    ok = emqx_utils:interactive_load(ConfigLoader),
    case erlang:function_exported(ConfigLoader, schema_module, 0) of
        true ->
            try_upgrade_conf(apply(ConfigLoader, schema_module, []), Conf);
        false ->
            %% this happens during emqx app standalone test
            Conf
    end.

try_upgrade_conf(SchemaModule, Conf) ->
    try
        apply(SchemaModule, upgrade_raw_conf, [Conf])
    catch
        ErrorType:Reason:Stack ->
            ?SLOG(warning, #{
                msg => "failed_to_upgrade_config",
                error_type => ErrorType,
                reason => Reason,
                stacktrace => Stack
            }),
            Conf
    end.

up_req({remove, _Opts}) -> '$remove';
up_req({{update, Req}, _Opts}) -> Req.

return_change_result(#conf_info{conf_key_path = ConfKeyPath, update_args = {{update, Req}, Opts}}) ->
    case Req =/= ?TOMBSTONE_CONFIG_CHANGE_REQ of
        true ->
            #{
                config => emqx_config:get(ConfKeyPath, undefined),
                raw_config => return_rawconf(ConfKeyPath, Opts)
            };
        false ->
            %% like remove, nothing to return
            #{}
    end;
return_change_result(#conf_info{update_args = {remove, _Opts}}) ->
    #{}.

return_rawconf(ConfKeyPath, #{rawconf_with_defaults := true}) ->
    FullRawConf = emqx_config:fill_defaults(emqx_config:get_raw([])),
    emqx_utils_maps:deep_get(bin_path(ConfKeyPath), FullRawConf);
return_rawconf(ConfKeyPath, _) ->
    emqx_config:get_raw(ConfKeyPath).

bin_path(ConfKeyPath) -> [bin(Key) || Key <- ConfKeyPath].

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(B) when is_binary(B) -> B.

atom(Bin) when is_binary(Bin), size(Bin) > 255 ->
    erlang:throw(
        iolist_to_binary(
            io_lib:format(
                "Name is too long."
                " Please provide a shorter name (<= 255 bytes)."
                " The name that is too long: \"~s\"",
                [Bin]
            )
        )
    );
atom(Bin) when is_binary(Bin) ->
    binary_to_atom(Bin, utf8);
atom(Str) when is_list(Str) ->
    list_to_atom(Str);
atom(Atom) when is_atom(Atom) ->
    Atom.

-dialyzer({nowarn_function, do_remove_handler/2}).
do_remove_handler(ConfKeyPath, Handlers) ->
    NewHandlers = emqx_utils_maps:deep_remove(ConfKeyPath ++ [?MOD], Handlers),
    remove_empty_leaf(ConfKeyPath, NewHandlers).

remove_empty_leaf([], Handlers) ->
    Handlers;
remove_empty_leaf(KeyPath, Handlers) ->
    case emqx_utils_maps:deep_find(KeyPath, Handlers) =:= {ok, #{}} of
        %% empty leaf
        true ->
            Handlers1 = emqx_utils_maps:deep_remove(KeyPath, Handlers),
            SubKeyPath = lists:sublist(KeyPath, length(KeyPath) - 1),
            remove_empty_leaf(SubKeyPath, Handlers1);
        false ->
            Handlers
    end.

assert_callback_function(Mod) ->
    emqx_utils:interactive_load(Mod),
    case
        erlang:function_exported(Mod, pre_config_update, 3) orelse
            erlang:function_exported(Mod, post_config_update, 5) orelse
            erlang:function_exported(Mod, pre_config_update, 4) orelse
            erlang:function_exported(Mod, post_config_update, 6)
    of
        true -> ok;
        false -> error(#{msg => "bad_emqx_config_handler_callback", module => Mod})
    end,
    ok.

-spec schema(module(), conf_key_path()) -> hocon_schema:schema().
schema(SchemaModule, [RootKey | _]) ->
    Roots = hocon_schema:roots(SchemaModule),
    {Field, Translations} =
        case lists:keyfind(bin(RootKey), 1, Roots) of
            {_, {Ref, ?REF(Ref)}} -> {Ref, ?R_REF(SchemaModule, Ref)};
            {_, {Name, Field0}} -> parse_translations(Field0, Name, SchemaModule);
            false -> throw({root_key_not_found, RootKey})
        end,
    #{
        roots => [Field],
        translations => Translations,
        validations => hocon_schema:validations(SchemaModule)
    }.

parse_translations(#{translate_to := TRs} = Field, Name, SchemaModule) ->
    {
        {Name, maps:remove(translate_to, Field)},
        lists:foldl(
            fun(T, Acc) ->
                Acc#{T => hocon_schema:translation(SchemaModule, T)}
            end,
            #{},
            TRs
        )
    };
parse_translations(Field, Name, _SchemaModule) ->
    {{Name, Field}, #{}}.

-spec load_prev_handlers() -> handlers().
load_prev_handlers() ->
    Handlers = application:get_env(emqx, ?MODULE, #{}),
    application:unset_env(emqx, ?MODULE),
    Handlers.

save_handlers(Handlers) ->
    application:set_env(emqx, ?MODULE, Handlers).

get_function_arity(_Module, _Callback, []) ->
    false;
get_function_arity(Module, Callback, [Arity | Opts]) ->
    %% ensure module is loaded
    ok = emqx_utils:interactive_load(Module),
    case erlang:function_exported(Module, Callback, Arity) of
        true -> Arity;
        false -> get_function_arity(Module, Callback, Opts)
    end.

conf_info_to_map(#conf_info{
    schema_mod = SchemaMod,
    conf_key_path = ConfKeyPath,
    update_args = UpdateArgs,
    cluster_rpc_opts = ClusterRpcOpts,
    handlers = Handlers
}) ->
    #{
        schema_mod => SchemaMod,
        conf_key_path => ConfKeyPath,
        update_args => UpdateArgs,
        update_req => up_req(UpdateArgs),
        cluster_rpc_opts => ClusterRpcOpts,
        handlers => Handlers
    }.
