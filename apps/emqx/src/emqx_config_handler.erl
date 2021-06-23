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
        , update_config/2
        , get_raw_config/0
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

-type update_request() :: term().
-type raw_config() :: hocon:config() | undefined.
-type config_key() :: atom().
-type handler_name() :: module().
-type config_key_path() :: [atom()].
-type handlers() :: #{config_key() => handlers(), ?MOD => handler_name()}.

-optional_callbacks([handle_update_config/2]).

-callback handle_update_config(update_request(), raw_config()) -> update_request().

-type state() :: #{
    handlers := handlers(),
    raw_config := raw_config(),
    atom() => term()
}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, {}, []).

-spec update_config(config_key_path(), update_request()) -> ok | {error, term()}.
update_config(ConfKeyPath, UpdateReq) ->
    gen_server:call(?MODULE, {update_config, ConfKeyPath, UpdateReq}).

-spec add_handler(config_key_path(), handler_name()) -> ok.
add_handler(ConfKeyPath, HandlerName) ->
    gen_server:call(?MODULE, {add_child, ConfKeyPath, HandlerName}).

-spec get_raw_config() -> raw_config().
get_raw_config() ->
    gen_server:call(?MODULE, get_raw_config).

%%============================================================================

-spec init(raw_config()) -> {ok, state()}.
init({}) ->
    {ok, RawConf} = hocon:load(emqx_conf_name(), #{format => richmap}),
    save_config_to_memory(RawConf),
    {ok, #{raw_config => hocon_schema:richmap_to_map(RawConf),
           handlers => #{?MOD => ?MODULE}}}.

handle_call(get_raw_config, _From, State = #{raw_config := RawConf}) ->
    {reply, RawConf, State};

handle_call({add_child, ConfKeyPath, HandlerName}, _From,
            State = #{handlers := Handlers}) ->
    {reply, ok, State#{handlers =>
        emqx_config:deep_put(ConfKeyPath, Handlers, #{?MOD => HandlerName})}};

handle_call({update_config, ConfKeyPath, UpdateReq}, _From,
        #{raw_config := RawConf, handlers := Handlers} = State) ->
    try {RootKeys, Conf} = do_update_config(ConfKeyPath, Handlers, RawConf, UpdateReq),
        {reply, save_configs(RootKeys, Conf), State}
    catch
        throw: Reason ->
            {reply, {error, Reason}, State};
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

do_update_config([], Handlers, OldConf, UpdateReq) ->
    call_handle_update_config(Handlers, OldConf, UpdateReq);
do_update_config([ConfKey | ConfKeyPath], Handlers,
        OldConf, UpdateReq) ->
    SubOldConf = maps:get(bin(ConfKey), OldConf, undefined),
    case maps:find(ConfKey, Handlers) of
        false -> throw({handler_not_found, ConfKey});
        {ok, SubHandlers} ->
            #{ConfKey => call_handle_update_config(Handlers, OldConf,
                do_update_config(ConfKeyPath, SubHandlers, SubOldConf, UpdateReq))}
    end.

call_handle_update_config(HandlerName, OldConf, UpdateReq) ->
    case erlang:function_exported(HandlerName, handle_update_config, 2) of
        true -> HandlerName:handle_update_config(UpdateReq, OldConf);
        false -> UpdateReq %% the default behaviour is overwriting the old config
    end.

%% callbacks for the top-level handler
handle_update_config(UpdateReq, OldConf) ->
    FullRawConf = merge_to_old_config(UpdateReq, OldConf),
    {maps:keys(UpdateReq), FullRawConf}.

%% default callback of config handlers
merge_to_old_config(UpdateReq, undefined) ->
    merge_to_old_config(UpdateReq, #{});
merge_to_old_config(UpdateReq, RawConf) ->
    maps:merge(RawConf, UpdateReq).

%%============================================================================
save_configs(RootKeys, Conf) ->
    save_config_to_memory(Conf),
    save_config_to_disk(RootKeys, Conf).

save_config_to_memory(Conf) ->
    {MappedEnvs, NewConf} = hocon_schema:map_translate(emqx_schema, Conf, #{}),
    lists:foreach(fun({AppName, Envs}) ->
            [application:set_env(AppName, Par, Val) || {Par, Val} <- Envs]
        end, MappedEnvs),
    emqx_config:put(hocon_schema:richmap_to_map(NewConf)).

save_config_to_disk(RootKeys, Conf) ->
    FileName = emqx_override_conf_name(),
    OldConf = read_old_config(FileName),
    %% We don't save the overall config to file, but only the sub configs
    %% under RootKeys
    write_new_config(FileName,
        maps:merge(OldConf, maps:with(RootKeys, Conf))).

write_new_config(FileName, Conf) ->
    case file:write_file(FileName, jsx:prettify(jsx:encode(Conf))) of
        ok -> ok;
        {error, Reason} ->
            logger:error("write to ~s failed, ~p", [FileName, Reason]),
            {error, Reason}
    end.

read_old_config(FileName) ->
    case file:read_file(FileName) of
        {ok, Text} ->
            try jsx:decode(Text) of
                Conf when is_map(Conf) -> Conf;
                _ -> #{}
            catch _Err : _Reason ->
                #{}
            end;
        _ -> #{}
    end.

emqx_conf_name() ->
    filename:join(["etc", "emqx.conf"]).

emqx_override_conf_name() ->
    filename:join(["data", "emqx_override.conf"]).

bin(A) when is_atom(A) -> list_to_binary(atom_to_list(A));
bin(B) when is_binary(B) -> B;
bin(S) when is_list(S) -> list_to_binary(S).
