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

-behaviour(gen_server).

%% API functions
-export([ start_link/1
        , start_handler/3
        , child_spec/2
        , child_spec/3
        , update_config/2
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

-type update_request() :: term().
-type raw_config() :: hocon:config() | undefined.
-type handler_name() :: module().
-type parent() :: handler_name() | root.
-type schema_root_name() :: atom() | string() | binary().
-type schema_root_names() :: all | [schema_root_name()].
-type key_path() :: [atom()].

-optional_callbacks([handle_update_config/2]).

-callback handle_update_config(update_request(), raw_config()) -> update_request().

-type state() :: #{
    handler_name := handler_name(),
    parent := parent(),
    raw_config := raw_config(),
    atom() => term()
}.

start_link(RawConfig) ->
    start_handler(?MODULE, root, RawConfig).

-spec start_handler(handler_name(), parent(), raw_config()) ->
    {ok, pid()} | {error, {already_started, pid()}} | {error, term()}.
start_handler(HandlerName, Parent, RawConfig) ->
    gen_server:start_link({local, HandlerName}, ?MODULE,
        {HandlerName, Parent, RawConfig}, []).

-spec child_spec(handler_name(), parent()) -> supervisor:child_spec().
child_spec(HandlerName, Parent) ->
    child_spec(HandlerName, Parent, undefined).

-spec child_spec(handler_name(), parent(), raw_config()) -> supervisor:child_spec().
child_spec(HandlerName, Parent, RawConfig) ->
    #{id => HandlerName,
      start => {?MODULE, start_handler, [HandlerName, Parent, RawConfig]},
      restart => permanent,
      type => worker,
      modules => [?MODULE]}.

-spec update_config(handler_name(), update_request()) -> ok | {error, term()}.
update_config(Handler, UpdateReq) ->
    gen_server:call(Handler, {update_config, UpdateReq}).

%%============================================================================

-spec init({handler_name(), parent(), raw_config()}) ->
    {ok, state()}.
init({HandlerName, Parent, RawConfig}) ->
    {ok, #{handler_name => HandlerName, parent => Parent, raw_config => RawConfig}}.

handle_call({update_config, UpdateReq}, _From, #{handler_name := HandlerName,
        parent := Parent, raw_config := OldConfig} = State) ->
    %% The Mod:handle_update_config/2 returns an integrated config-request that
    %% is going to be handled by its parent handler
    IntegratedConf = case erlang:function_exported(HandlerName, handle_update_config, 2) of
        true -> HandlerName:handle_update_config(UpdateReq, OldConfig);
        false -> merge_to_old_config(UpdateReq, OldConfig)
    end,
    case continue_update_config(Parent, IntegratedConf) of
        ok -> {reply, ok, State#{raw_config => IntegratedConf}};
        Error -> {reply, Error, State}
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

%% callbacks for the top-level handler
handle_update_config(UpdateReq, OldConfig) ->
    FullRawConfig = merge_to_old_config(UpdateReq, OldConfig),
    {maps:keys(UpdateReq), FullRawConfig}.

%% default callback of config handlers
merge_to_old_config(UpdateReq, undefined) ->
    merge_to_old_config(UpdateReq, #{});
merge_to_old_config(UpdateReq, RawConfig) ->
    maps:merge(RawConfig, UpdateReq).

%%============================================================================

continue_update_config(root, {RootKeys, Config}) ->
    %% this is the top-level handler, we save the configs to disk and memory
    save_configs(RootKeys, Config);
continue_update_config(Parent, ConfigReq) ->
    %% update config to the parent handler
    update_config(Parent, ConfigReq).

save_configs(RootKeys, Config) ->
    {AppEnvs, MappedConf} = hocon_schema:map(emqx_schema, Config, all,
        #{atom_key => true, return_plain => true}),
    lists:foreach(fun({AppName, Envs}) ->
            [application:set_env(AppName, Par, Val) || {Par, Val} <- Envs]
        end, AppEnvs),
    %% overwrite the config to disk and memory
    emqx_config:put(MappedConf),
    save_config_to_disk(RootKeys, Config).

save_config_to_disk(RootKeys, Config) ->
    FileName = emqx_override_file_name(),
    OldConfig = read_old_config(FileName),
    %% We don't save the overall config to file, but only the sub configs
    %% under RootKeys
    write_new_config(FileName,
        maps:merge(OldConfig, maps:with(RootKeys, Config))).

write_new_config(FileName, Config) ->
    case file:write_file(FileName, jsx:prettify(jsx:encode(Config))) of
        ok -> ok;
        {error, Reason} ->
            logger:error("write to ~s failed, ~p", [Filename, Reason]),
            {error, Reason}
    end.

read_old_config(FileName) ->
    case file:read_file(FileName) of
        {ok, Text} ->
            try jsx:decode(Text) of
                Conf when is_map(Conf) -> Conf;
                _ -> #{};
            catch Err: Reason ->
                #{}
            end;
        _ -> #{}
    end.

emqx_override_file_name() ->
    filename:join(["data", "emqx_override.conf"]).

bin(A) when is_atom(A) -> list_to_binary(atom_to_list(A));
bin(B) when is_binary(B) -> B;
bin(S) when is_list(S) -> list_to_binary(S).
