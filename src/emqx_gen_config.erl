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

-module(emqx_gen_config).

-include("logger.hrl").

-export([generate_config/4]).

generate_config(EmqCfg, PluginsEtc, DestDir, SchemaDir) ->
    case mergeconf(EmqCfg, PluginsEtc, DestDir) of
        {error, Error} ->
            exit(Error);
        AppConf ->
            Schemas = filelib:wildcard("*.schema", SchemaDir),
            Schemas1 = lists:map(
                fun(Schema) ->
                    filename:join(SchemaDir, Schema)
                end, Schemas),
            Schemas2 = cuttlefish_schema:files(Schemas1),
            case cuttlefish_generator:map(Schemas2, cuttlefish_conf:file(AppConf)) of
                {error, Error1} ->
                    exit(Error1);
                AllConfigs ->
                    [[application:set_env(App, Key, Val) || {Key, Val} <- Configs]|| {App, Configs} <- AllConfigs]
            end
    end.

mergeconf(EmqCfg, PluginsEtc, DestDir) ->
    filelib:ensure_dir(DestDir),
    AppConf = filename:join(DestDir, appconf()),
    case file:open(AppConf, [write]) of
        {ok, DestFd} ->
            CfgFiles = [EmqCfg | cfgfiles(PluginsEtc)],
            lists:foreach(fun(CfgFile) ->
                            {ok, CfgData} = file:read_file(CfgFile),
                            file:write(DestFd, CfgData),
                            file:write(DestFd, <<"\n">>)
                          end, CfgFiles),
            file:close(DestFd),
            AppConf;
        {error, Error} ->
            ?LOG(error, "Error opening file: ", [Error]),
            {error, Error}
    end.

appconf() ->
    {{Y, M, D}, {H, MM, S}} = calendar:local_time(),
    lists:flatten(
      io_lib:format(
        "app.~4..0w.~2..0w.~2..0w.~2..0w.~2..0w.~2..0w.conf", [Y, M, D, H, MM, S])).

cfgfiles(Dir) ->
    [filename:join(Dir, CfgFile) || CfgFile <- filelib:wildcard("*.conf", Dir)].