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

%% emqx_config_handler callbacks
-export([ handle_update_config/2
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(MOD, {mod}).

-type handler_name() :: module().
-type handlers() :: #{emqx_config:config_key() => handlers(), ?MOD => handler_name()}.

-optional_callbacks([ handle_update_config/2
                    , post_update_config/2
                    ]).

-callback handle_update_config(emqx_config:update_request(), emqx_config:raw_config()) ->
    emqx_config:update_request().

-callback post_update_config(emqx_config:config(), emqx_config:config()) -> any().

-type state() :: #{
    handlers := handlers(),
    atom() => term()
}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, {}, []).

-spec update_config(emqx_config:config_key_path(), emqx_config:update_request(),
        emqx_config:raw_config()) ->
    ok | {error, term()}.
update_config(ConfKeyPath, UpdateReq, RawConfig) ->
    gen_server:call(?MODULE, {update_config, ConfKeyPath, UpdateReq, RawConfig}).

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

handle_call({update_config, ConfKeyPath, UpdateReq, RawConf}, _From,
            #{handlers := Handlers} = State) ->
    OldConf = emqx_config:get(),
    try {RootKeys, Conf} = do_update_config(ConfKeyPath, Handlers, RawConf, UpdateReq),
        Result = emqx_config:save_configs(Conf, #{overridden_keys => RootKeys}),
        do_post_update_config(ConfKeyPath, Handlers, OldConf, emqx_config:get()),
        {reply, Result, State}
    catch
        Error : Reason : ST ->
            ?LOG(error, "update config failed: ~p", [{Error, Reason, ST}]),
            {reply, {error, Reason}, State}
    end;

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

do_update_config([], Handlers, OldRawConf, UpdateReq) ->
    call_handle_update_config(Handlers, OldRawConf, UpdateReq);
do_update_config([ConfKey | ConfKeyPath], Handlers, OldRawConf, UpdateReq) ->
    SubOldRawConf = get_sub_config(bin(ConfKey), OldRawConf),
    SubHandlers = maps:get(ConfKey, Handlers, #{}),
    NewUpdateReq = do_update_config(ConfKeyPath, SubHandlers, SubOldRawConf, UpdateReq),
    call_handle_update_config(Handlers, OldRawConf, #{bin(ConfKey) => NewUpdateReq}).

do_post_update_config([], Handlers, OldConf, NewConf) ->
    call_post_update_config(Handlers, OldConf, NewConf);
do_post_update_config([ConfKey | ConfKeyPath], Handlers, OldConf, NewConf) ->
    SubOldConf = get_sub_config(ConfKey, OldConf),
    SubNewConf = get_sub_config(ConfKey, NewConf),
    SubHandlers = maps:get(ConfKey, Handlers, #{}),
    _ = do_post_update_config(ConfKeyPath, SubHandlers, SubOldConf, SubNewConf),
    call_post_update_config(Handlers, OldConf, NewConf).

get_sub_config(ConfKey, Conf) when is_map(Conf) ->
    maps:get(ConfKey, Conf, undefined);
get_sub_config(_, _Conf) -> %% the Conf is a primitive
    undefined.

call_handle_update_config(Handlers, OldRawConf, UpdateReq) ->
    HandlerName = maps:get(?MOD, Handlers, undefined),
    case erlang:function_exported(HandlerName, handle_update_config, 2) of
        true -> HandlerName:handle_update_config(UpdateReq, OldRawConf);
        false -> merge_to_old_config(UpdateReq, OldRawConf)
    end.

call_post_update_config(Handlers, OldConf, NewConf) ->
    HandlerName = maps:get(?MOD, Handlers, undefined),
    case erlang:function_exported(HandlerName, post_update_config, 2) of
        true -> _ = HandlerName:post_update_config(NewConf, OldConf);
        false -> ok
    end.

%% callbacks for the top-level handler
handle_update_config(UpdateReq, OldConf) ->
    FullRawConf = merge_to_old_config(UpdateReq, OldConf),
    {maps:keys(UpdateReq), FullRawConf}.

%% The default callback of config handlers
%% the behaviour is overwriting the old config if:
%%   1. the old config is undefined
%%   2. either the old or the new config is not of map type
%% the behaviour is merging the new the config to the old config if they are maps.
merge_to_old_config(UpdateReq, RawConf) when is_map(UpdateReq), is_map(RawConf) ->
    maps:merge(RawConf, UpdateReq);
merge_to_old_config(UpdateReq, _RawConf) ->
    UpdateReq.

bin(A) when is_atom(A) -> list_to_binary(atom_to_list(A));
bin(B) when is_binary(B) -> B;
bin(S) when is_list(S) -> list_to_binary(S).
