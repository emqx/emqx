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
        , start_handler/4
        , update_config/2
        , child_spec/4
        ]).

%% emqx_config_handler callbacks
% -export([ handle_update_config/2
%         ]).

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
%-type key_path() :: [atom()].

-optional_callbacks([handle_update_config/2]).

-callback handle_update_config(update_request(), raw_config()) -> raw_config().

-type state() :: #{
    handler_name := handler_name(),
    parent := parent(),
    schema_mod := module(),
    schema_root_names := schema_root_names(),
    raw_config := raw_config(),
    atom() => term()
}.

start_link() ->
    start_handler(?MODULE, root, emqx_schema, all).

-spec start_handler(handler_name(), parent(), module(), schema_root_names()) ->
    {ok, pid()} | {error, {already_started, pid()}} | {error, term()}.
start_handler(HandlerName, Parent, SchemaMod, SchemaRootNames) ->
    gen_server:start_link({local, HandlerName}, ?MODULE,
        {HandlerName, Parent, SchemaMod, SchemaRootNames}, []).

-spec child_spec(handler_name(), parent(), module(), schema_root_names()) ->
    supervisor:child_spec().
child_spec(HandlerName, Parent, SchemaMod, SchemaRootNames) ->
    #{id => HandlerName,
      start => {?MODULE, start_handler, [HandlerName, Parent, SchemaMod, SchemaRootNames]},
      restart => permanent,
      type => worker,
      modules => [?MODULE]}.

-spec update_config(handler_name(), update_request()) -> ok.
update_config(Handler, UpdateReq) ->
    case is_process_alive(whereis(Handler)) of
        true -> gen_server:cast(Handler, {handle_update_config, UpdateReq});
        false -> error({not_alive, Handler})
    end.

%%============================================================================

-spec init({handler_name(), parent(), module(), schema_root_names()}) ->
    {ok, state()}.
init({HandlerName, Parent, SchemaMod, SchemaRootNames, SchemaRootNames}) ->
    {ok, #{handler_name => HandlerName, parent => Parent, schema_mod => SchemaMod,
           raw_config => undefined, schema_root_names => SchemaRootNames}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({handle_update_config, UpdateReq}, #{handler_name := HandlerName,
        parent := Parent, raw_config := OldConfig, schema_mod := SchemaMod,
        schema_root_names := SchemaRootName} = State) ->
    %% accumulate the config map of this config handler
    SubConfigMap = case erlang:function_exported(HandlerName, handle_update_config, 2) of
        true -> HandlerName:handle_update_config(UpdateReq, OldConfig);
        false -> merge_to_old_config(UpdateReq, OldConfig)
    end,
    {_, MappedConf} = hocon_schema:map(SchemaMod, SubConfigMap, SchemaRootName,
        #{atom_key => true, return_plain => true}),
    %% then notify the parent handler
    maybe_update_to_parent(Parent, MappedConf),
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
-spec maybe_update_to_parent(parent(), update_request()) -> ok.
maybe_update_to_parent(root, UpdateReq) ->
    %% overwrite the full config map to disk and memory
    emqx_config:put(UpdateReq),
    save_config_to_disk(UpdateReq);
maybe_update_to_parent(Parent, UpdateReq) ->
    update_config(Parent, UpdateReq).

%% default callback of config handlers
merge_to_old_config(UpdateReq, undefined) ->
    merge_to_old_config(UpdateReq, #{});
merge_to_old_config(UpdateReq, RawConfig) ->
    maps:merge(RawConfig, UpdateReq).

save_config_to_disk(ConfigMap) ->
    Filename = filename:join([emqx_data_dir(), "emqx_override.conf"]),
    case file:write_file(Filename, jsx:encode(ConfigMap)) of
        ok -> ok;
        {error, Reason} -> logger:error("write to ~s failed, ~p", [Filename, Reason])
    end.

emqx_data_dir() ->
    %emqx_config:get([node, data_dir])
    "data".
