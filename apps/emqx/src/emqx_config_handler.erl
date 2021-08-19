%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-behaviour(gen_server).

%% API functions
-export([ start_link/0
        , add_handler/2
        , update_config/3
        , merge_to_old_config/2
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(MOD, {mod}).

-define(ATOM_CONF_PATH(PATH, EXP, EXP_ON_FAIL),
    try [safe_atom(Key) || Key <- PATH] of
        AtomKeyPath -> EXP
    catch
        error:badarg -> EXP_ON_FAIL
    end).

-type handler_name() :: module().
-type handlers() :: #{emqx_config:config_key() => handlers(), ?MOD => handler_name()}.

-optional_callbacks([ pre_config_update/2
                    , post_config_update/3
                    ]).

-callback pre_config_update(emqx_config:update_request(), emqx_config:raw_config()) ->
    {ok, emqx_config:update_request()} | {error, term()}.

-callback post_config_update(emqx_config:update_request(), emqx_config:config(),
    emqx_config:config()) -> ok | {ok, Result::any()} | {error, Reason::term()}.

-type state() :: #{
    handlers := handlers(),
    atom() => term()
}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, {}, []).

-spec update_config(module(), emqx_config:config_key_path(), emqx_config:update_args()) ->
    {ok, emqx_config:update_result()} | {error, emqx_config:update_error()}.
update_config(SchemaModule, ConfKeyPath, UpdateArgs) ->
    ?ATOM_CONF_PATH(ConfKeyPath, gen_server:call(?MODULE, {change_config, SchemaModule,
        AtomKeyPath, UpdateArgs}), {error, ConfKeyPath}).

-spec add_handler(emqx_config:config_key_path(), handler_name()) -> ok.
add_handler(ConfKeyPath, HandlerName) ->
    gen_server:call(?MODULE, {add_child, ConfKeyPath, HandlerName}).

%%============================================================================

-spec init(term()) -> {ok, state()}.
init(_) ->
    {ok, #{handlers => #{?MOD => ?MODULE}}}.

handle_call({add_child, ConfKeyPath, HandlerName}, _From,
            State = #{handlers := Handlers}) ->
    {reply, ok, State#{handlers =>
        emqx_map_lib:deep_put(ConfKeyPath, Handlers, #{?MOD => HandlerName})}};

handle_call({change_config, SchemaModule, ConfKeyPath, UpdateArgs}, _From,
            #{handlers := Handlers} = State) ->
    OldConf = emqx_config:get_root(ConfKeyPath),
    OldRawConf = emqx_config:get_root_raw(ConfKeyPath),
    Reply = try
        case process_update_request(ConfKeyPath, OldRawConf, Handlers, UpdateArgs) of
            {ok, NewRawConf, OverrideConf} ->
                check_and_save_configs(SchemaModule, ConfKeyPath, Handlers, NewRawConf, OldConf,
                    OverrideConf, UpdateArgs);
            {error, Result} ->
                {error, Result}
        end
    catch Error:Reason:ST ->
        ?LOG(error, "change_config failed: ~p", [{Error, Reason, ST}]),
        {error, Reason}
    end,
    {reply, Reply, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

process_update_request(ConfKeyPath, OldRawConf, _Handlers, {remove, _Opts}) ->
    BinKeyPath = bin_path(ConfKeyPath),
    NewRawConf = emqx_map_lib:deep_remove(BinKeyPath, OldRawConf),
    OverrideConf = emqx_map_lib:deep_remove(BinKeyPath, emqx_config:read_override_conf()),
    {ok, NewRawConf, OverrideConf};
process_update_request(ConfKeyPath, OldRawConf, Handlers, {{update, UpdateReq}, _Opts}) ->
    case do_update_config(ConfKeyPath, Handlers, OldRawConf, UpdateReq) of
        {ok, NewRawConf} ->
            OverrideConf = update_override_config(NewRawConf),
            {ok, NewRawConf, OverrideConf};
        Error -> Error
    end.

do_update_config([], Handlers, OldRawConf, UpdateReq) ->
    call_pre_config_update(Handlers, OldRawConf, UpdateReq);
do_update_config([ConfKey | ConfKeyPath], Handlers, OldRawConf, UpdateReq) ->
    SubOldRawConf = get_sub_config(bin(ConfKey), OldRawConf),
    SubHandlers = maps:get(ConfKey, Handlers, #{}),
    case do_update_config(ConfKeyPath, SubHandlers, SubOldRawConf, UpdateReq) of
        {ok, NewUpdateReq} ->
            call_pre_config_update(Handlers, OldRawConf, #{bin(ConfKey) => NewUpdateReq});
        Error ->
            Error
    end.

check_and_save_configs(SchemaModule, ConfKeyPath, Handlers, NewRawConf, OldConf, OverrideConf,
        UpdateArgs) ->
    {AppEnvs, CheckedConf} = emqx_config:check_config(SchemaModule, NewRawConf),
    case do_post_config_update(ConfKeyPath, Handlers, OldConf, CheckedConf, UpdateArgs, #{}) of
        {ok, Result0} ->
            case save_configs(ConfKeyPath, AppEnvs, CheckedConf, NewRawConf, OverrideConf,
                    UpdateArgs) of
                {ok, Result1} ->
                    {ok, Result1#{post_config_update => Result0}};
                Error -> Error
            end;
        Error -> Error
    end.

do_post_config_update([], Handlers, OldConf, NewConf, UpdateArgs, Result) ->
    call_post_config_update(Handlers, OldConf, NewConf, up_req(UpdateArgs), Result);
do_post_config_update([ConfKey | ConfKeyPath], Handlers, OldConf, NewConf, UpdateArgs, Result) ->
    SubOldConf = get_sub_config(ConfKey, OldConf),
    SubNewConf = get_sub_config(ConfKey, NewConf),
    SubHandlers = maps:get(ConfKey, Handlers, #{}),
    case do_post_config_update(ConfKeyPath, SubHandlers, SubOldConf, SubNewConf, UpdateArgs,
            Result) of
        {ok, Result1} ->
            call_post_config_update(Handlers, OldConf, NewConf, up_req(UpdateArgs), Result1);
        Error -> Error
    end.

get_sub_config(ConfKey, Conf) when is_map(Conf) ->
    maps:get(ConfKey, Conf, undefined);
get_sub_config(_, _Conf) -> %% the Conf is a primitive
    undefined.

call_pre_config_update(Handlers, OldRawConf, UpdateReq) ->
    HandlerName = maps:get(?MOD, Handlers, undefined),
    case erlang:function_exported(HandlerName, pre_config_update, 2) of
        true ->
            case HandlerName:pre_config_update(UpdateReq, OldRawConf) of
                {ok, NewUpdateReq} -> {ok, NewUpdateReq};
                {error, Reason} -> {error, {pre_config_update, HandlerName, Reason}}
            end;
        false -> merge_to_old_config(UpdateReq, OldRawConf)
    end.

call_post_config_update(Handlers, OldConf, NewConf, UpdateReq, Result) ->
    HandlerName = maps:get(?MOD, Handlers, undefined),
    case erlang:function_exported(HandlerName, post_config_update, 3) of
        true ->
            case HandlerName:post_config_update(UpdateReq, NewConf, OldConf) of
                ok -> {ok, Result};
                {ok, Result1} -> {ok, Result#{HandlerName => Result1}};
                {error, Reason} -> {error, {post_config_update, HandlerName, Reason}}
            end;
        false -> {ok, Result}
    end.

save_configs(ConfKeyPath, AppEnvs, CheckedConf, NewRawConf, OverrideConf, {_Cmd, Opts}) ->
    case emqx_config:save_configs(AppEnvs, CheckedConf, NewRawConf, OverrideConf) of
        ok -> {ok, #{config => emqx_config:get(ConfKeyPath),
                     raw_config => return_rawconf(ConfKeyPath, Opts)}};
        {error, Reason} -> {error, {save_configs, Reason}}
    end.

%% The default callback of config handlers
%% the behaviour is overwriting the old config if:
%%   1. the old config is undefined
%%   2. either the old or the new config is not of map type
%% the behaviour is merging the new the config to the old config if they are maps.
merge_to_old_config(UpdateReq, RawConf) when is_map(UpdateReq), is_map(RawConf) ->
    {ok, maps:merge(RawConf, UpdateReq)};
merge_to_old_config(UpdateReq, _RawConf) ->
    {ok, UpdateReq}.

update_override_config(RawConf) ->
    OldConf = emqx_config:read_override_conf(),
    maps:merge(OldConf, RawConf).

up_req({remove, _Opts}) -> '$remove';
up_req({{update, Req}, _Opts}) -> Req.

return_rawconf(ConfKeyPath, #{rawconf_with_defaults := true}) ->
    FullRawConf = emqx_config:fill_defaults(emqx_config:get_raw([])),
    emqx_map_lib:deep_get(bin_path(ConfKeyPath), FullRawConf);
return_rawconf(ConfKeyPath, _) ->
    emqx_config:get_raw(ConfKeyPath).

bin_path(ConfKeyPath) -> [bin(Key) || Key <- ConfKeyPath].

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(B) when is_binary(B) -> B.

safe_atom(Bin) when is_binary(Bin) ->
    binary_to_existing_atom(Bin, latin1);
safe_atom(Str) when is_list(Str) ->
    list_to_existing_atom(Str);
safe_atom(Atom) when is_atom(Atom) ->
    Atom.