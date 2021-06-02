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
-module(emqx_connector).

-export([ load_from_config/1
        , load_connectors/1
        , load_connector/1
        ]).

load_from_config(Filename) ->
    case hocon:load(Filename, #{format => map}) of
        {ok, #{<<"connectors">> := Connectors}} ->
            load_connectors(Connectors);
        {error, Reason} ->
            error(Reason)
    end.

load_connectors(Connectors) ->
    lists:foreach(fun load_connector/1, Connectors).

load_connector(Config) ->
    case emqx_resource:load_instance_from_config(Config) of
        {ok, _} -> ok;
        {error, already_created} -> ok;
        {error, Reason} ->
            error({load_connector, Reason})
    end.
