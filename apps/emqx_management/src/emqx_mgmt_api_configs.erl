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

-module(emqx_mgmt_api_configs).

-behaviour(minirest_api).

-export([api_spec/0]).

-export([ config/2
        ]).

-define(CONFIG_NAMES, [log, rpc, broker, zones, sysmon, alarm]).

-define(PARAM_CONF_PATH, [#{
    name => conf_path,
    in => query,
    description => <<"The config path separated by '.' character">>,
    required => false,
    schema => #{type => string, default => <<".">>}
}]).

api_spec() ->
    {config_apis(), config_schemas()}.

config_schemas() ->
    [#{RootName => #{type => object}} %% TODO: generate this from hocon schema
     || RootName <- ?CONFIG_NAMES].

config_apis() ->
    [config_api(RootName) || RootName <- ?CONFIG_NAMES].

config_api(RootName) when is_atom(RootName) ->
    RootNameStr = atom_to_list(RootName),
    Descr = fun(Prefix) -> list_to_binary(Prefix ++ " " ++ RootNameStr) end,
    Metadata = #{
        get => #{
            description => Descr("Get configs for"),
            parameters => ?PARAM_CONF_PATH,
            responses => #{
                <<"200">> => emqx_mgmt_util:response_schema(<<"The config value for log">>,
                    RootName)
            }
        },
        delete => #{
            description => Descr("Remove configs for"),
            parameters => ?PARAM_CONF_PATH,
            responses => #{
                <<"200">> => emqx_mgmt_util:response_schema(<<"Remove configs successfully">>),
                <<"400">> => emqx_mgmt_util:response_error_schema(
                    <<"It's not able to remove the config">>, ['INVALID_OPERATION'])
            }
        },
        put => #{
            description => Descr("Update configs for"),
            'requestBody' => emqx_mgmt_util:request_body_schema(RootName),
            responses => #{
                <<"200">> => emqx_mgmt_util:response_schema(<<"Update configs successfully">>,
                    RootName),
                <<"400">> => emqx_mgmt_util:response_error_schema(
                    <<"Update configs failed">>, ['UPDATE_FAILED'])
            }
        }
    },
    {"/configs/" ++ RootNameStr, Metadata, config}.

%%%==============================================================================================
%% parameters trans
config(get, Req) ->
    %% TODO: query the config specified by the query string param 'conf_path'
    Conf = emqx_config:get_raw([root_name_from_path(Req) | conf_path_from_querystr(Req)]),
    {200, Conf};

config(delete, Req) ->
    %% TODO: remove the config specified by the query string param 'conf_path'
    emqx_config:remove([root_name_from_path(Req) | conf_path_from_querystr(Req)]),
    {200};

config(put, Req) ->
    RootName = root_name_from_path(Req),
    ok = emqx_config:update([RootName], http_body(Req)),
    {200, emqx_config:get_raw([RootName])}.

root_name_from_path(Req) ->
    <<"/api/v5/configs/", RootName/binary>> = cowboy_req:path(Req),
    string:trim(RootName, trailing, "/").

conf_path_from_querystr(Req) ->
    case proplists:get_value(<<"conf_path">>, cowboy_req:parse_qs(Req)) of
        undefined -> [];
        Path -> [string:lexemes(Path, ". ")]
    end.

http_body(Req) ->
    {ok, Body, _} = cowboy_req:read_body(Req),
    Body.
