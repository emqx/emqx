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

-module(emqx_retainer_api).

-rest_api(#{name   => lookup_config,
            method => 'GET',
            path   => "/retainer",
            func   => lookup_config,
            descr  => "lookup retainer config"
           }).

-rest_api(#{name   => update_config,
            method => 'PUT',
            path   => "/retainer",
            func   => update_config,
            descr  => "update retainer config"
           }).

-export([ lookup_config/2
        , update_config/2
        ]).

lookup_config(_Bindings, _Params) ->
    Config = emqx_config:get([emqx_retainer]),
    return({ok, Config}).

update_config(_Bindings, Params) ->
    try
        ConfigList = proplists:get_value(<<"emqx_retainer">>, Params),
        {ok, RawConf} = hocon:binary(jsx:encode(#{<<"emqx_retainer">> => ConfigList}),
                                     #{format => richmap}),
        RichConf = hocon_schema:check(emqx_retainer_schema, RawConf, #{atom_key => true}),
        #{emqx_retainer := Conf} = hocon_schema:richmap_to_map(RichConf),
        Action = proplists:get_value(<<"action">>, Params, undefined),
        do_update_config(Action, Conf),
        return()
    catch _:_:Reason ->
            return({error, Reason})
    end.

%%------------------------------------------------------------------------------
%% Interval Funcs
%%------------------------------------------------------------------------------
do_update_config(undefined, Config) ->
    emqx_retainer:update_config(Config);
do_update_config(<<"test">>, _) ->
    ok.

%%    TODO: V5 API
return() ->
    ok.
return(_) ->
    ok.
