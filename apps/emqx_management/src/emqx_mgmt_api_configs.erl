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
        , config_reset/2
        ]).

-define(PARAM_CONF_PATH, [#{
    name => conf_path,
    in => query,
    description => <<"The config path separated by '.' character">>,
    required => false,
    schema => #{type => string, default => <<".">>}
}]).

-define(TEXT_BODY(DESCR, SCHEMA), #{
    description => list_to_binary(DESCR),
    content => #{
        <<"text/plain">> => #{
            schema => SCHEMA
        }
    }
}).

-define(PREFIX, "/configs").
-define(PREFIX_RESET, "/configs_reset").

-define(MAX_DEPTH, 1).

-define(ERR_MSG(MSG), list_to_binary(io_lib:format("~p", [MSG]))).

api_spec() ->
    {config_apis() ++ [config_reset_api()], []}.

config_apis() ->
    [config_api(ConfPath, Schema) || {ConfPath, Schema} <-
     get_conf_schema(emqx_config:get([]), ?MAX_DEPTH)].

config_api(ConfPath, Schema) ->
    Path = path_join(ConfPath),
    Descr = fun(Str) ->
        list_to_binary([Str, " ", path_join(ConfPath, ".")])
    end,
    Metadata = #{
        get => #{
            description => Descr("Get configs for"),
            responses => #{
                <<"200">> => ?TEXT_BODY("Get configs successfully", Schema),
                <<"404">> => emqx_mgmt_util:response_error_schema(
                    <<"Config not found">>, ['NOT_FOUND'])
            }
        },
        put => #{
            description => Descr("Update configs for"),
            'requestBody' => ?TEXT_BODY("The format of the request body is depend on the 'conf_path' parameter in the query string", Schema),
            responses => #{
                <<"200">> => ?TEXT_BODY("Update configs successfully", Schema),
                <<"400">> => emqx_mgmt_util:response_error_schema(
                    <<"Update configs failed">>, ['UPDATE_FAILED'])
            }
        }
    },
    {?PREFIX ++ "/" ++ Path, Metadata, config}.

config_reset_api() ->
    Metadata = #{
        post => #{
            description => <<"Reset the config entry specified by the query string parameter `conf_path`.<br/>
- For a config entry that has default value, this resets it to the default value;
- For a config entry that has no default value, an error 400 will be returned">>,
            parameters => ?PARAM_CONF_PATH,
            responses => #{
                %% We only return "200" rather than the new configs that has been changed, as
                %% the schema of the changed configs is depends on the request parameter
                %% `conf_path`, it cannot be defined here.
                <<"200">> => emqx_mgmt_util:response_schema(<<"Reset configs successfully">>),
                <<"400">> => emqx_mgmt_util:response_error_schema(
                    <<"It's not able to reset the config">>, ['INVALID_OPERATION'])
            }
        }
    },
    {?PREFIX_RESET, Metadata, config_reset}.

%%%==============================================================================================
%% parameters trans
config(get, Req) ->
    Path = conf_path(Req),
    case emqx_map_lib:deep_find(Path, get_full_config()) of
        {ok, Conf} ->
            {200, Conf};
        {not_found, _, _} ->
            {404, #{code => 'NOT_FOUND', message => <<"Config cannot found">>}}
    end;

config(put, Req) ->
    Path = conf_path(Req),
    {ok, _, RawConf} = emqx_config:update(Path, http_body(Req),
        #{rawconf_with_defaults => true}),
    {200, emqx_map_lib:deep_get(Path, emqx_map_lib:jsonable_map(RawConf))}.

config_reset(post, Req) ->
    %% reset the config specified by the query string param 'conf_path'
    Path = conf_path_reset(Req) ++ conf_path_from_querystr(Req),
    case emqx_config:reset(Path, #{}) of
        {ok, _, _} -> {200};
        {error, Reason} ->
            {400, ?ERR_MSG(Reason)}
    end.

get_full_config() ->
    emqx_map_lib:jsonable_map(
        emqx_config:fill_defaults(emqx_config:get_raw([]))).

conf_path_from_querystr(Req) ->
    case proplists:get_value(<<"conf_path">>, cowboy_req:parse_qs(Req)) of
        undefined -> [];
        Path -> string:lexemes(Path, ". ")
    end.

conf_path(Req) ->
    <<"/api/v5", ?PREFIX, Path/binary>> = cowboy_req:path(Req),
    string:lexemes(Path, "/ ").

conf_path_reset(Req) ->
    <<"/api/v5", ?PREFIX_RESET, Path/binary>> = cowboy_req:path(Req),
    string:lexemes(Path, "/ ").

http_body(Req) ->
    {ok, Body, _} = cowboy_req:read_body(Req),
    try jsx:decode(Body, [{return_maps, true}])
    catch error:badarg -> Body
    end.

get_conf_schema(Conf, MaxDepth) ->
    get_conf_schema([], maps:to_list(Conf), [], MaxDepth).

get_conf_schema(_BasePath, [], Result, _MaxDepth) ->
    Result;
get_conf_schema(BasePath, [{Key, Conf} | Confs], Result, MaxDepth) ->
    Path = BasePath ++ [Key],
    Depth = length(Path),
    Result1 = case is_map(Conf) of
        true when Depth < MaxDepth ->
            get_conf_schema(Path, maps:to_list(Conf), Result, MaxDepth);
        true when Depth >= MaxDepth -> Result;
        false -> Result
    end,
    get_conf_schema(BasePath, Confs, [{Path, gen_schema(Conf)} | Result1], MaxDepth).

%% TODO: generate from hocon schema
gen_schema(Conf) when is_boolean(Conf) ->
    #{type => boolean};
gen_schema(Conf) when is_binary(Conf); is_atom(Conf) ->
    #{type => string};
gen_schema(Conf) when is_number(Conf) ->
    #{type => number};
gen_schema(Conf) when is_list(Conf) ->
    #{type => array, items => case Conf of
            [] -> #{}; %% don't know the type
            _ -> gen_schema(hd(Conf))
        end};
gen_schema(Conf) when is_map(Conf) ->
    #{type => object, properties =>
        maps:map(fun(_K, V) -> gen_schema(V) end, Conf)};
gen_schema(_Conf) ->
    %% the conf is not of JSON supported type, it may have been converted
    %% by the hocon schema
    #{type => string}.

path_join(Path) ->
    path_join(Path, "/").

path_join([P], _Sp) -> str(P);
path_join([P | Path], Sp) ->
    str(P) ++ Sp ++ path_join(Path, Sp).

str(S) when is_list(S) -> S;
str(S) when is_binary(S) -> binary_to_list(S);
str(S) when is_atom(S) -> atom_to_list(S).
