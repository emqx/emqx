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
-module(emqx_resource_api).

-export([ list_instances/1
        , format_data/1
        , stringnify/1
        ]).

list_instances(Filter) ->
    [format_data(Data) || Data <- emqx_resource:list_instances_verbose(), Filter(Data)].

format_data(#{id := Id, mod := Mod, status := Status, config := Config}) ->
    #{id => Id, status => Status, resource_type => Mod,
      config => emqx_resource:call_jsonify(Mod, Config)}.

stringnify(Bin) when is_binary(Bin) -> Bin;
stringnify(Str) when is_list(Str) -> list_to_binary(Str);
stringnify(Reason) ->
    iolist_to_binary(io_lib:format("~p", [Reason])).
