%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Hot Configuration
%%
%% TODO: How to persist the configuration?
%%
%% 1. Store in mnesia database?
%% 2. Store in dets?
%% 3. Store in data/app.config?

-module(emqx_config).

-export([populate/1]).

-export([ read/1
        , write/2
        , dump/2
        , reload/1
        ]).

-export([ set/3
        , get/2
        , get/3
        , get_env/1
        , get_env/2
        ]).

-type(env() :: {atom(), term()}).

-define(APP, emqx).

%% @doc Get environment
-spec(get_env(Key :: atom()) -> term() | undefined).
get_env(Key) ->
    get_env(Key, undefined).

-spec(get_env(Key :: atom(), Default :: term()) -> term()).
get_env(Key, Default) ->
    application:get_env(?APP, Key, Default).

%% TODO:
populate(_App) ->
    ok.

%% @doc Read the configuration of an application.
-spec(read(atom()) -> {ok, list(env())} | {error, term()}).
read(App) ->
    %% TODO:
    %% 1. Read the app.conf from etc folder
    %% 2. Cuttlefish to read the conf
    %% 3. Return the terms and schema
    % {error, unsupported}.
    {ok, read_(App)}.

%% @doc Reload configuration of an application.
-spec(reload(atom()) -> ok | {error, term()}).
reload(_App) ->
    %% TODO
    %% 1. Read the app.conf from etc folder
    %% 2. Cuttlefish to generate config terms.
    %% 3. set/3 to apply the config
    ok.

-spec(write(atom(), list(env())) -> ok | {error, term()}).
write(_App, _Terms) -> ok.
    % Configs = lists:map(fun({Key, Val}) ->
    %     {cuttlefish_variable:tokenize(binary_to_list(Key)), binary_to_list(Val)}
    % end, Terms),
    % Path = lists:concat([code:priv_dir(App), "/", App, ".schema"]),
    % Schema = cuttlefish_schema:files([Path]),
    % case cuttlefish_generator:map(Schema, Configs) of
    %     [{App, Configs1}] ->
    %         emqx_cli_config:write_config(App, Configs),
    %         lists:foreach(fun({Key, Val}) -> application:set_env(App, Key, Val) end, Configs1);
    %     _ ->
    %         error
    % end.

-spec(dump(atom(), list(env())) -> ok | {error, term()}).
dump(_App, _Terms) ->
    %% TODO
    ok.

-spec(set(atom(), list(), list()) -> ok).
set(_App, _Par, _Val) -> ok.
    % emqx_cli_config:run(["config",
    %                         "set",
    %                         lists:concat([Par, "=", Val]),
    %                         lists:concat(["--app=", App])]).

-spec(get(atom(), list()) -> undefined | {ok, term()}).
get(_App, _Par) -> error(no_impl).
    % case emqx_cli_config:get_cfg(App, Par) of
    %     undefined -> undefined;
    %     Val -> {ok, Val}
    % end.

-spec(get(atom(), list(), atom()) -> term()).
get(_App, _Par, _Def) -> error(no_impl).
    % emqx_cli_config:get_cfg(App, Par, Def).


read_(_App) -> error(no_impl).
    % Configs = emqx_cli_config:read_config(App),
    % Path = lists:concat([code:priv_dir(App), "/", App, ".schema"]),
    % case filelib:is_file(Path) of
    %     false ->
    %         [];
    %     true ->
    %         {_, Mappings, _} = cuttlefish_schema:files([Path]),
    %         OptionalCfg = lists:foldl(fun(Map, Acc) ->
    %             Key = cuttlefish_mapping:variable(Map),
    %             case proplists:get_value(Key, Configs) of
    %                 undefined ->
    %                     [{cuttlefish_variable:format(Key), "", cuttlefish_mapping:doc(Map), false} | Acc];
    %                 _ -> Acc
    %             end
    %         end, [], Mappings),
    %         RequiredCfg = lists:foldl(fun({Key, Val}, Acc) ->
    %             case lists:keyfind(Key, 2, Mappings) of
    %                 false -> Acc;
    %                 Map ->
    %                     [{cuttlefish_variable:format(Key), Val, cuttlefish_mapping:doc(Map), true} | Acc]
    %             end
    %         end, [], Configs),
    %         RequiredCfg ++ OptionalCfg
    % end.
