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
-export([ start_link/0
        , start_handler/3
        , update_config/2
        , child_spec/3
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

-type config() :: term().
-type config_map() :: #{atom() => config()} | [config_map()].
-type handler_name() :: module() | top.
-type key_path() :: [atom()].

-optional_callbacks([handle_update_config/2]).

-callback handle_update_config(config(), config_map()) -> config_map().

-record(state, {
    handler_name :: handler_name(),
    parent :: handler_name(),
    key_path :: key_path()
}).

start_link() ->
    start_handler(?MODULE, top, []).

-spec start_handler(handler_name(), handler_name(), key_path()) ->
    {ok, pid()} | {error, {already_started, pid()}} | {error, term()}.
start_handler(HandlerName, Parent, ConfKeyPath) ->
    gen_server:start_link({local, HandlerName}, ?MODULE, {HandlerName, Parent, ConfKeyPath}, []).

-spec child_spec(module(), handler_name(), key_path()) -> supervisor:child_spec().
child_spec(Mod, Parent, KeyPath) ->
    #{id => Mod,
      start => {?MODULE, start_handler, [Mod, Parent, KeyPath]},
      restart => permanent,
      type => worker,
      modules => [?MODULE]}.

-spec update_config(handler_name(), config()) -> ok.
update_config(top, Config) ->
    emqx_config:put(Config),
    save_config_to_disk(Config);
update_config(Handler, Config) ->
    case is_process_alive(whereis(Handler)) of
        true -> gen_server:cast(Handler, {handle_update_config, Config});
        false -> error({not_alive, Handler})
    end.

%%============================================================================
%% callbacks of emqx_config_handler (the top-level handler)
handle_update_config(Config, undefined) ->
    handle_update_config(Config, #{});
handle_update_config(Config, OldConfigMap) ->
    %% simply merge the config to the overall config
    maps:merge(OldConfigMap, Config).

init({HandlerName, Parent, ConfKeyPath}) ->
    {ok, #state{handler_name = HandlerName, parent = Parent, key_path = ConfKeyPath}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({handle_update_config, Config}, #state{handler_name = HandlerName,
        parent = Parent, key_path = ConfKeyPath} = State) ->
    %% accumulate the config map of this config handler
    OldConfigMap = emqx_config:get(ConfKeyPath, undefined),
    SubConfigMap = case erlang:function_exported(HandlerName, handle_update_config, 2) of
        true -> HandlerName:handle_update_config(Config, OldConfigMap);
        false -> wrap_sub_config(ConfKeyPath, Config)
    end,
    %% then notify the parent handler
    update_config(Parent, SubConfigMap),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%============================================================================
save_config_to_disk(ConfigMap) ->
    Filename = filename:join([emqx_data_dir(), "emqx_override.conf"]),
    case file:write_file(Filename, jsx:encode(ConfigMap)) of
        ok -> ok;
        {error, Reason} -> logger:error("write to ~s failed, ~p", [Filename, Reason])
    end.

emqx_data_dir() ->
    %emqx_config:get([node, data_dir])
    "data".

wrap_sub_config(ConfKeyPath, Config) ->
    emqx_config:deep_put(ConfKeyPath, #{}, Config).
