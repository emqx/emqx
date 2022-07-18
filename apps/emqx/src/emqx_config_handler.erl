%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(gen_server).

%% API functions
-export([
    start_link/0,
    stop/0,
    add_handler/2,
    remove_handler/1,
    update_config/3,
    get_raw_cluster_override_conf/0,
    info/0,
    merge_to_old_config/2
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

-define(MOD, {mod}).
-define(WKEY, '?').

-type handler_name() :: module().
-type handlers() :: #{emqx_config:config_key() => handlers(), ?MOD => handler_name()}.

-optional_callbacks([
    pre_config_update/3,
    post_config_update/5
]).

-callback pre_config_update([atom()], emqx_config:update_request(), emqx_config:raw_config()) ->
    {ok, emqx_config:update_request()} | {error, term()}.

-callback post_config_update(
    [atom()],
    emqx_config:update_request(),
    emqx_config:config(),
    emqx_config:config(),
    emqx_config:app_envs()
) ->
    ok | {ok, Result :: any()} | {error, Reason :: term()}.

-type state() :: #{
    handlers := handlers(),
    atom() => term()
}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, {}, []).

stop() ->
    gen_server:stop(?MODULE).

-spec update_config(module(), emqx_config:config_key_path(), emqx_config:update_args()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
update_config(SchemaModule, ConfKeyPath, UpdateArgs) ->
    %% force convert the path to a list of atoms, as there maybe some wildcard names/ids in the path
    AtomKeyPath = [atom(Key) || Key <- ConfKeyPath],
    gen_server:call(?MODULE, {change_config, SchemaModule, AtomKeyPath, UpdateArgs}, infinity).

-spec add_handler(emqx_config:config_key_path(), handler_name()) ->
    ok | {error, {conflict, list()}}.
add_handler(ConfKeyPath, HandlerName) ->
    assert_callback_function(HandlerName),
    gen_server:call(?MODULE, {add_handler, ConfKeyPath, HandlerName}).

%% @doc Remove handler asynchronously
-spec remove_handler(emqx_config:config_key_path()) -> ok.
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
handle_call(
    {change_config, SchemaModule, ConfKeyPath, UpdateArgs},
    _From,
    #{handlers := Handlers} = State
) ->
    Result = handle_update_request(SchemaModule, ConfKeyPath, Handlers, UpdateArgs),
    {reply, Result, State};
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
    maps:fold(
        fun
            (K, #{?MOD := _}, Acc) -> [K | Acc];
            (_K, #{}, Acc) -> Acc;
            (?MOD, _, Acc) -> Acc
        end,
        [],
        Handlers
    ).

handle_update_request(SchemaModule, ConfKeyPath, Handlers, UpdateArgs) ->
    try
        do_handle_update_request(SchemaModule, ConfKeyPath, Handlers, UpdateArgs)
    catch
        throw:Reason ->
            {error, Reason};
        Error:Reason:ST ->
            ?SLOG(error, #{
                msg => "change_config_crashed",
                exception => Error,
                reason => Reason,
                update_req => UpdateArgs,
                module => SchemaModule,
                key_path => ConfKeyPath,
                stacktrace => ST
            }),
            {error, {config_update_crashed, Reason}}
    end.

do_handle_update_request(SchemaModule, ConfKeyPath, Handlers, UpdateArgs) ->
    case process_update_request(ConfKeyPath, Handlers, UpdateArgs) of
        {ok, NewRawConf, OverrideConf, Opts} ->
            check_and_save_configs(
                SchemaModule,
                ConfKeyPath,
                Handlers,
                NewRawConf,
                OverrideConf,
                UpdateArgs,
                Opts
            );
        {error, Result} ->
            {error, Result}
    end.

process_update_request([_], _Handlers, {remove, _Opts}) ->
    {error, "remove_root_is_forbidden"};
process_update_request(ConfKeyPath, _Handlers, {remove, Opts}) ->
    OldRawConf = emqx_config:get_root_raw(ConfKeyPath),
    BinKeyPath = bin_path(ConfKeyPath),
    NewRawConf = emqx_map_lib:deep_remove(BinKeyPath, OldRawConf),
    OverrideConf = remove_from_override_config(BinKeyPath, Opts),
    {ok, NewRawConf, OverrideConf, Opts};
process_update_request(ConfKeyPath, Handlers, {{update, UpdateReq}, Opts}) ->
    OldRawConf = emqx_config:get_root_raw(ConfKeyPath),
    case do_update_config(ConfKeyPath, Handlers, OldRawConf, UpdateReq) of
        {ok, NewRawConf} ->
            OverrideConf = update_override_config(NewRawConf, Opts),
            {ok, NewRawConf, OverrideConf, Opts};
        Error ->
            Error
    end.

do_update_config(ConfKeyPath, Handlers, OldRawConf, UpdateReq) ->
    do_update_config(ConfKeyPath, Handlers, OldRawConf, UpdateReq, []).

do_update_config([], Handlers, OldRawConf, UpdateReq, ConfKeyPath) ->
    call_pre_config_update(Handlers, OldRawConf, UpdateReq, ConfKeyPath);
do_update_config(
    [ConfKey | SubConfKeyPath],
    Handlers,
    OldRawConf,
    UpdateReq,
    ConfKeyPath0
) ->
    ConfKeyPath = ConfKeyPath0 ++ [ConfKey],
    ConfKeyBin = bin(ConfKey),
    SubOldRawConf = get_sub_config(ConfKeyBin, OldRawConf),
    SubHandlers = get_sub_handlers(ConfKey, Handlers),
    case do_update_config(SubConfKeyPath, SubHandlers, SubOldRawConf, UpdateReq, ConfKeyPath) of
        {ok, NewUpdateReq} -> merge_to_old_config(#{ConfKeyBin => NewUpdateReq}, OldRawConf);
        Error -> Error
    end.

check_and_save_configs(
    SchemaModule,
    ConfKeyPath,
    Handlers,
    NewRawConf,
    OverrideConf,
    UpdateArgs,
    Opts
) ->
    OldConf = emqx_config:get_root(ConfKeyPath),
    Schema = schema(SchemaModule, ConfKeyPath),
    {AppEnvs, NewConf} = emqx_config:check_config(Schema, NewRawConf),
    case do_post_config_update(ConfKeyPath, Handlers, OldConf, NewConf, AppEnvs, UpdateArgs, #{}) of
        {ok, Result0} ->
            remove_from_local_if_cluster_change(ConfKeyPath, Opts),
            ok = emqx_config:save_configs(AppEnvs, NewConf, NewRawConf, OverrideConf, Opts),
            Result1 = return_change_result(ConfKeyPath, UpdateArgs),
            {ok, Result1#{post_config_update => Result0}};
        Error ->
            Error
    end.

do_post_config_update(ConfKeyPath, Handlers, OldConf, NewConf, AppEnvs, UpdateArgs, Result) ->
    do_post_config_update(
        ConfKeyPath,
        Handlers,
        OldConf,
        NewConf,
        AppEnvs,
        UpdateArgs,
        Result,
        []
    ).

do_post_config_update(
    [],
    Handlers,
    OldConf,
    NewConf,
    AppEnvs,
    UpdateArgs,
    Result,
    ConfKeyPath
) ->
    call_post_config_update(
        Handlers,
        OldConf,
        NewConf,
        AppEnvs,
        up_req(UpdateArgs),
        Result,
        ConfKeyPath
    );
do_post_config_update(
    [ConfKey | SubConfKeyPath],
    Handlers,
    OldConf,
    NewConf,
    AppEnvs,
    UpdateArgs,
    Result,
    ConfKeyPath0
) ->
    ConfKeyPath = ConfKeyPath0 ++ [ConfKey],
    SubOldConf = get_sub_config(ConfKey, OldConf),
    SubNewConf = get_sub_config(ConfKey, NewConf),
    SubHandlers = get_sub_handlers(ConfKey, Handlers),
    case
        do_post_config_update(
            SubConfKeyPath,
            SubHandlers,
            SubOldConf,
            SubNewConf,
            AppEnvs,
            UpdateArgs,
            Result,
            ConfKeyPath
        )
    of
        {ok, Result1} ->
            call_post_config_update(
                Handlers,
                OldConf,
                NewConf,
                AppEnvs,
                up_req(UpdateArgs),
                Result1,
                ConfKeyPath
            );
        Error ->
            Error
    end.

get_sub_handlers(ConfKey, Handlers) ->
    case maps:find(ConfKey, Handlers) of
        error -> maps:get(?WKEY, Handlers, #{});
        {ok, SubHandlers} -> SubHandlers
    end.

get_sub_config(ConfKey, Conf) when is_map(Conf) ->
    maps:get(ConfKey, Conf, undefined);
%% the Conf is a primitive
get_sub_config(_, _Conf) ->
    undefined.

call_pre_config_update(#{?MOD := HandlerName}, OldRawConf, UpdateReq, ConfKeyPath) ->
    case erlang:function_exported(HandlerName, pre_config_update, 3) of
        true ->
            case HandlerName:pre_config_update(ConfKeyPath, UpdateReq, OldRawConf) of
                {ok, NewUpdateReq} -> {ok, NewUpdateReq};
                {error, Reason} -> {error, {pre_config_update, HandlerName, Reason}}
            end;
        false ->
            merge_to_old_config(UpdateReq, OldRawConf)
    end;
call_pre_config_update(_Handlers, OldRawConf, UpdateReq, _ConfKeyPath) ->
    merge_to_old_config(UpdateReq, OldRawConf).

call_post_config_update(
    #{?MOD := HandlerName},
    OldConf,
    NewConf,
    AppEnvs,
    UpdateReq,
    Result,
    ConfKeyPath
) ->
    case erlang:function_exported(HandlerName, post_config_update, 5) of
        true ->
            case
                HandlerName:post_config_update(
                    ConfKeyPath,
                    UpdateReq,
                    NewConf,
                    OldConf,
                    AppEnvs
                )
            of
                ok -> {ok, Result};
                {ok, Result1} -> {ok, Result#{HandlerName => Result1}};
                {error, Reason} -> {error, {post_config_update, HandlerName, Reason}}
            end;
        false ->
            {ok, Result}
    end;
call_post_config_update(
    _Handlers,
    _OldConf,
    _NewConf,
    _AppEnvs,
    _UpdateReq,
    Result,
    _ConfKeyPath
) ->
    {ok, Result}.

%% The default callback of config handlers
%% the behaviour is overwriting the old config if:
%%   1. the old config is undefined
%%   2. either the old or the new config is not of map type
%% the behaviour is merging the new the config to the old config if they are maps.

merge_to_old_config(UpdateReq, RawConf) when is_map(UpdateReq), is_map(RawConf) ->
    {ok, maps:merge(RawConf, UpdateReq)};
merge_to_old_config(UpdateReq, _RawConf) ->
    {ok, UpdateReq}.

%% local-override.conf priority is higher than cluster-override.conf
%% If we want cluster to take effect, we must remove the local.
remove_from_local_if_cluster_change(BinKeyPath, #{override_to := cluster} = Opts) ->
    Opts1 = Opts#{override_to => local},
    Local = remove_from_override_config(BinKeyPath, Opts1),
    _ = emqx_config:save_to_override_conf(Local, Opts1),
    ok;
remove_from_local_if_cluster_change(_BinKeyPath, _Opts) ->
    ok.

remove_from_override_config(_BinKeyPath, #{persistent := false}) ->
    undefined;
remove_from_override_config(BinKeyPath, Opts) ->
    OldConf = emqx_config:read_override_conf(Opts),
    emqx_map_lib:deep_remove(BinKeyPath, OldConf).

update_override_config(_RawConf, #{persistent := false}) ->
    undefined;
update_override_config(RawConf, Opts) ->
    OldConf = emqx_config:read_override_conf(Opts),
    maps:merge(OldConf, RawConf).

up_req({remove, _Opts}) -> '$remove';
up_req({{update, Req}, _Opts}) -> Req.

return_change_result(ConfKeyPath, {{update, _Req}, Opts}) ->
    #{
        config => emqx_config:get(ConfKeyPath),
        raw_config => return_rawconf(ConfKeyPath, Opts)
    };
return_change_result(_ConfKeyPath, {remove, _Opts}) ->
    #{}.

return_rawconf(ConfKeyPath, #{rawconf_with_defaults := true}) ->
    FullRawConf = emqx_config:fill_defaults(emqx_config:get_raw([])),
    emqx_map_lib:deep_get(bin_path(ConfKeyPath), FullRawConf);
return_rawconf(ConfKeyPath, _) ->
    emqx_config:get_raw(ConfKeyPath).

bin_path(ConfKeyPath) -> [bin(Key) || Key <- ConfKeyPath].

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(B) when is_binary(B) -> B.

atom(Bin) when is_binary(Bin) ->
    binary_to_atom(Bin, utf8);
atom(Str) when is_list(Str) ->
    list_to_atom(Str);
atom(Atom) when is_atom(Atom) ->
    Atom.

-dialyzer({nowarn_function, do_remove_handler/2}).
do_remove_handler(ConfKeyPath, Handlers) ->
    NewHandlers = emqx_map_lib:deep_remove(ConfKeyPath ++ [?MOD], Handlers),
    remove_empty_leaf(ConfKeyPath, NewHandlers).

remove_empty_leaf([], Handlers) ->
    Handlers;
remove_empty_leaf(KeyPath, Handlers) ->
    case emqx_map_lib:deep_find(KeyPath, Handlers) =:= {ok, #{}} of
        %% empty leaf
        true ->
            Handlers1 = emqx_map_lib:deep_remove(KeyPath, Handlers),
            SubKeyPath = lists:sublist(KeyPath, length(KeyPath) - 1),
            remove_empty_leaf(SubKeyPath, Handlers1);
        false ->
            Handlers
    end.

assert_callback_function(Mod) ->
    case
        erlang:function_exported(Mod, pre_config_update, 3) orelse
            erlang:function_exported(Mod, post_config_update, 5)
    of
        true -> ok;
        false -> error(#{msg => "bad_emqx_config_handler_callback", module => Mod})
    end,
    ok.

-spec schema(module(), emqx_map_lib:config_key_path()) -> hocon_schema:schema().
schema(SchemaModule, [RootKey | _]) ->
    Roots = hocon_schema:roots(SchemaModule),
    {Field, Translations} =
        case lists:keyfind(bin(RootKey), 1, Roots) of
            {_, {Ref, ?REF(Ref)}} -> {Ref, ?R_REF(SchemaModule, Ref)};
            {_, {Name, Field0}} -> parse_translations(Field0, Name, SchemaModule)
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

load_prev_handlers() ->
    Handlers = application:get_env(emqx, ?MODULE, #{}),
    application:unset_env(emqx, ?MODULE),
    Handlers.

save_handlers(Handlers) ->
    application:set_env(emqx, ?MODULE, Handlers).
