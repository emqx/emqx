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

-define(PARAM_CONF_PATH, [#{
    name => conf_path,
    in => query,
    description => <<"The config path separated by '.' character">>,
    required => false,
    schema => #{type => string, default => <<".">>}
}]).

-define(TEXT_BODY(DESCR), #{
    description => list_to_binary(DESCR),
    content => #{
        <<"text/plain">> => #{
            schema => #{}
        }
    }
}).

api_spec() ->
    {config_apis(), []}.

config_apis() ->
    [config_api()].

config_api() ->
    Metadata = #{
        get => #{
            description => <<"Get configs">>,
            parameters => ?PARAM_CONF_PATH,
            responses => #{
                <<"200">> => ?TEXT_BODY("Get configs successfully"),
                <<"404">> => emqx_mgmt_util:response_error_schema(
                    <<"Config not found">>, ['NOT_FOUND'])
            }
        },
        delete => #{
            description => <<"Remove configs for">>,
            parameters => ?PARAM_CONF_PATH,
            responses => #{
                <<"200">> => emqx_mgmt_util:response_schema(<<"Remove configs successfully">>),
                <<"400">> => emqx_mgmt_util:response_error_schema(
                    <<"It's not able to remove the config">>, ['INVALID_OPERATION'])
            }
        },
        put => #{
            description => <<"Update configs for">>,
            parameters => ?PARAM_CONF_PATH,
            'requestBody' => ?TEXT_BODY("The format of the request body is depend on the 'conf_path' parameter in the query string"),
            responses => #{
                <<"200">> => ?TEXT_BODY("Update configs successfully"),
                <<"400">> => emqx_mgmt_util:response_error_schema(
                    <<"Update configs failed">>, ['UPDATE_FAILED'])
            }
        }
    },
    {"/configs", Metadata, config}.

%%%==============================================================================================
%% parameters trans
config(get, Req) ->
    %% TODO: query the config specified by the query string param 'conf_path'
    case emqx_config:find_raw(conf_path_from_querystr(Req)) of
        {ok, Conf} ->
            {200, Conf};
        {not_found, _, _} ->
            {404, #{code => 'NOT_FOUND', message => <<"Config cannot found">>}}
    end;

config(delete, Req) ->
    %% TODO: remove the config specified by the query string param 'conf_path'
    emqx_config:remove(conf_path_from_querystr(Req)),
    {200};

config(put, Req) ->
    Path = [binary_to_atom(Key, latin1) || Key <- conf_path_from_querystr(Req)],
    ok = emqx_config:update(Path, http_body(Req)),
    {200, emqx_config:get_raw(Path)}.

conf_path_from_querystr(Req) ->
    case proplists:get_value(<<"conf_path">>, cowboy_req:parse_qs(Req)) of
        undefined -> [];
        Path -> string:lexemes(Path, ". ")
    end.

http_body(Req) ->
    {ok, Body, _} = cowboy_req:read_body(Req),
    try jsx:decode(Body, [{return_maps, true}])
    catch error:badarg -> Body
    end.
