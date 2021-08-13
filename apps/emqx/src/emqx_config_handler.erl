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

-export_type([update_opts/0, update_cmd/0, update_args/0]).
-type handler_name() :: module().
-type handlers() :: #{emqx_config:config_key() => handlers(), ?MOD => handler_name()}.
-type update_cmd() :: {update, emqx_config:update_request()} | remove.
-type update_opts() :: #{
        %% fill the default values into the rawconf map
        rawconf_with_defaults => boolean()
    }.
-type update_args() :: {update_cmd(), Opts :: update_opts()}.

-optional_callbacks([ pre_config_update/2
                    , post_config_update/3
                    ]).

-callback pre_config_update(emqx_config:update_request(), emqx_config:raw_config()) ->
    emqx_config:update_request().

-callback post_config_update(emqx_config:update_request(), emqx_config:config(),
    emqx_config:config()) -> any().

-type state() :: #{
    handlers := handlers(),
    atom() => term()
}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, {}, []).

-spec update_config(module(), emqx_config:config_key_path(), update_args()) ->
    {ok, emqx_config:config(), emqx_config:raw_config()} | {error, term()}.
update_config(SchemaModule, ConfKeyPath, UpdateArgs) ->
    gen_server:call(?MODULE, {change_config, SchemaModule, ConfKeyPath, UpdateArgs}).

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

handle_call({change_config, SchemaModule, ConfKeyPath, {_Cmd, Opts} = UpdateArgs}, _From,
            #{handlers := Handlers} = State) ->
    OldConf = emqx_config:get([]),
    OldRawConf = emqx_config:get_raw([]),
    Result = try
        {NewRawConf, OverrideConf} = process_upadate_request(ConfKeyPath, OldRawConf,
            Handlers, UpdateArgs),
        {AppEnvs, CheckedConf} = emqx_config:check_config(SchemaModule, NewRawConf),
        _ = do_post_config_update(ConfKeyPath, Handlers, OldConf, CheckedConf, UpdateArgs),
        case emqx_config:save_configs(AppEnvs, CheckedConf, NewRawConf, OverrideConf) of
            ok -> {ok, emqx_config:get([]), return_rawconf(Opts)};
            Err -> Err
        end
    catch Error:Reason:ST ->
        ?LOG(error, "change_config failed: ~p", [{Error, Reason, ST}]),
        {error, Reason}
    end,
    {reply, Result, State};

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

process_upadate_request(ConfKeyPath, OldRawConf, _Handlers, {remove, _Opts}) ->
    BinKeyPath = bin_path(ConfKeyPath),
    NewRawConf = emqx_map_lib:deep_remove(BinKeyPath, OldRawConf),
    OverrideConf = emqx_map_lib:deep_remove(BinKeyPath, emqx_config:read_override_conf()),
    {NewRawConf, OverrideConf};
process_upadate_request(ConfKeyPath, OldRawConf, Handlers, {{update, UpdateReq}, _Opts}) ->
    NewRawConf = do_update_config(ConfKeyPath, Handlers, OldRawConf, UpdateReq),
    OverrideConf = update_override_config(NewRawConf),
    {NewRawConf, OverrideConf}.

do_update_config([], Handlers, OldRawConf, UpdateReq) ->
    call_pre_config_update(Handlers, OldRawConf, UpdateReq);
do_update_config([ConfKey | ConfKeyPath], Handlers, OldRawConf, UpdateReq) ->
    SubOldRawConf = get_sub_config(bin(ConfKey), OldRawConf),
    SubHandlers = maps:get(ConfKey, Handlers, #{}),
    NewUpdateReq = do_update_config(ConfKeyPath, SubHandlers, SubOldRawConf, UpdateReq),
    call_pre_config_update(Handlers, OldRawConf, #{bin(ConfKey) => NewUpdateReq}).

do_post_config_update([], Handlers, OldConf, NewConf, UpdateArgs) ->
    call_post_config_update(Handlers, OldConf, NewConf, up_req(UpdateArgs));
do_post_config_update([ConfKey | ConfKeyPath], Handlers, OldConf, NewConf, UpdateArgs) ->
    SubOldConf = get_sub_config(ConfKey, OldConf),
    SubNewConf = get_sub_config(ConfKey, NewConf),
    SubHandlers = maps:get(ConfKey, Handlers, #{}),
    _ = do_post_config_update(ConfKeyPath, SubHandlers, SubOldConf, SubNewConf, UpdateArgs),
    call_post_config_update(Handlers, OldConf, NewConf, up_req(UpdateArgs)).

get_sub_config(ConfKey, Conf) when is_map(Conf) ->
    maps:get(ConfKey, Conf, undefined);
get_sub_config(_, _Conf) -> %% the Conf is a primitive
    undefined.

call_pre_config_update(Handlers, OldRawConf, UpdateReq) ->
    HandlerName = maps:get(?MOD, Handlers, undefined),
    case erlang:function_exported(HandlerName, pre_config_update, 2) of
        true -> HandlerName:pre_config_update(UpdateReq, OldRawConf);
        false -> merge_to_old_config(UpdateReq, OldRawConf)
    end.

call_post_config_update(Handlers, OldConf, NewConf, UpdateReq) ->
    HandlerName = maps:get(?MOD, Handlers, undefined),
    case erlang:function_exported(HandlerName, post_config_update, 3) of
        true -> HandlerName:post_config_update(UpdateReq, NewConf, OldConf);
        false -> ok
    end.

%% The default callback of config handlers
%% the behaviour is overwriting the old config if:
%%   1. the old config is undefined
%%   2. either the old or the new config is not of map type
%% the behaviour is merging the new the config to the old config if they are maps.
merge_to_old_config(UpdateReq, RawConf) when is_map(UpdateReq), is_map(RawConf) ->
    maps:merge(RawConf, UpdateReq);
merge_to_old_config(UpdateReq, _RawConf) ->
    UpdateReq.

update_override_config(RawConf) ->
    OldConf = emqx_config:read_override_conf(),
    maps:merge(OldConf, RawConf).

up_req({remove, _Opts}) -> '$remove';
up_req({{update, Req}, _Opts}) -> Req.

return_rawconf(#{rawconf_with_defaults := true}) ->
    emqx_config:fill_defaults(emqx_config:get_raw([]));
return_rawconf(_) ->
    emqx_config:get_raw([]).

bin_path(ConfKeyPath) -> [bin(Key) || Key <- ConfKeyPath].

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(B) when is_binary(B) -> B.
