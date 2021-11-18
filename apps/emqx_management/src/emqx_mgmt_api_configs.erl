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

-include_lib("hocon/include/hoconsc.hrl").
-behaviour(minirest_api).

-export([api_spec/0]).
-export([paths/0, schema/1, fields/1]).

-export([config/3, config_reset/3, configs/3, get_full_config/0]).

-export([get_conf_schema/2, gen_schema/1]).

-define(PREFIX, "/configs/").
-define(PREFIX_RESET, "/configs_reset/").
-define(ERR_MSG(MSG), list_to_binary(io_lib:format("~p", [MSG]))).
-define(EXCLUDES, [listeners, node, cluster, gateway, rule_engine]).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE).

paths() ->
    ["/configs", "/configs_reset/:rootname"] ++
    lists:map(fun({Name, _Type}) -> ?PREFIX ++ to_list(Name) end, config_list(?EXCLUDES)).

schema("/configs") ->
    #{
        'operationId' => configs,
        get => #{
            tags => [conf],
            description =>
<<"Get all the configurations of the specified node, including hot and non-hot updatable items.">>,
            parameters => [
                {node, hoconsc:mk(typerefl:atom(),
                    #{in => query, required => false, example => <<"emqx@127.0.0.1">>,
                        desc =>
 <<"Node's name: If you do not fill in the fields, this node will be used by default.">>})}],
            responses => #{
                200 => config_list([])
            }
        }
    };
schema("/configs_reset/:rootname") ->
    Paths = lists:map(fun({Path, _}) -> Path end, config_list(?EXCLUDES)),
    #{
        'operationId' => config_reset,
        post => #{
            tags => [conf],
            description =>
<<"Reset the config entry specified by the query string parameter `conf_path`.<br/>
- For a config entry that has default value, this resets it to the default value;
- For a config entry that has no default value, an error 400 will be returned">>,
            %% We only return "200" rather than the new configs that has been changed, as
            %% the schema of the changed configs is depends on the request parameter
            %% `conf_path`, it cannot be defined here.
            parameters => [
                {rootname, hoconsc:mk( hoconsc:enum(Paths)
                                     , #{in => path, example => <<"authorization">>})},
                {conf_path, hoconsc:mk(typerefl:binary(),
                    #{in => query, required => false, example => <<"cache.enable">>,
                        desc => <<"The config path separated by '.' character">>})}],
            responses => #{
                200 => <<"Rest config successfully">>,
                400 => emqx_dashboard_swagger:error_codes(['NO_DEFAULT_VALUE', 'REST_FAILED'])
            }
        }
    };
schema(Path) ->
    {Root, Schema} = find_schema(Path),
    #{
        'operationId' => config,
        get => #{
            tags => [conf],
            description => iolist_to_binary([ <<"Get the sub-configurations under *">>
                                            , Root
                                            , <<"*">>]),
            responses => #{
                200 => Schema,
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], <<"config not found">>)
            }
        },
        put => #{
            tags => [conf],
            description => iolist_to_binary([ <<"Update the sub-configurations under *">>
                                            , Root
                                            , <<"*">>]),
            'requestBody' => Schema,
            responses => #{
                200 => Schema,
                400 => emqx_dashboard_swagger:error_codes(['UPDATE_FAILED'])
            }
        }
    }.

find_schema(Path) ->
    [_, _Prefix, Root | _] = string:split(Path, "/", all),
    Configs = config_list(?EXCLUDES),
    case lists:keyfind(Root, 1, Configs) of
        {Root, Schema} -> {Root, Schema};
        false ->
            RootAtom = list_to_existing_atom(Root),
            {Root, element(2, lists:keyfind(RootAtom, 1, Configs))}
    end.

%% we load all configs from emqx_conf_schema, some of them are defined as local ref
%% we need redirect to emqx_conf_schema.
%% such as hoconsc:ref("node") to hoconsc:ref(emqx_conf_schema, "node")
fields(Field) -> emqx_conf_schema:fields(Field).

%%%==============================================================================================
%% HTTP API Callbacks
config(get, _Params, Req) ->
    Path = conf_path(Req),
    case emqx_map_lib:deep_find(Path, get_full_config()) of
        {ok, Conf} ->
            {200, Conf};
        {not_found, _, _} ->
            {404, #{code => 'NOT_FOUND', message => <<"Config cannot found">>}}
    end;

config(put, #{body := Body}, Req) ->
    Path = conf_path(Req),
    case emqx:update_config(Path, Body, #{rawconf_with_defaults => true}) of
        {ok, #{raw_config := RawConf}} ->
            {200, emqx_map_lib:jsonable_map(RawConf)};
        {error, Reason} ->
            {400, #{code => 'UPDATE_FAILED', message => ?ERR_MSG(Reason)}}
    end.

config_reset(post, _Params, Req) ->
    %% reset the config specified by the query string param 'conf_path'
    Path = conf_path_reset(Req) ++ conf_path_from_querystr(Req),
    case emqx:reset_config(Path, #{}) of
        {ok, _} -> {200};
        {error, no_default_value} ->
            {400, #{code => 'NO_DEFAULT_VALUE', message => <<"No Default Value.">>}};
        {error, Reason} ->
            {400, #{code => 'REST_FAILED', message => ?ERR_MSG(Reason)}}
    end.

configs(get, Params, _Req) ->
    Node = maps:get(node, Params, node()),
    case
        lists:member(Node, mria_mnesia:running_nodes())
        andalso
        rpc:call(Node, ?MODULE, get_full_config, [])
    of
        false ->
            Message = list_to_binary(io_lib:format("Bad node ~p, reason not found", [Node])),
            {500, #{code => 'BAD_NODE', message => Message}};
        {error, {badrpc, R}} ->
            Message = list_to_binary(io_lib:format("Bad node ~p, reason ~p", [Node, R])),
            {500, #{code => 'BAD_NODE', message => Message}};
        Res ->
            {200, Res}
    end.

conf_path_reset(Req) ->
    <<"/api/v5", ?PREFIX_RESET, Path/binary>> = cowboy_req:path(Req),
    string:lexemes(Path, "/ ").

get_full_config() ->
    emqx_map_lib:jsonable_map(
        emqx_config:fill_defaults(emqx:get_raw_config([]))).

conf_path_from_querystr(Req) ->
    case proplists:get_value(<<"conf_path">>, cowboy_req:parse_qs(Req)) of
        undefined -> [];
        Path -> string:lexemes(Path, ". ")
    end.

config_list(Exclude) ->
    Roots = emqx_conf_schema:roots(),
    lists:foldl(fun(Key, Acc) -> lists:delete(Key, Acc) end, Roots, Exclude).

to_list(L) when is_list(L) -> L;
to_list(Atom) when is_atom(Atom) -> atom_to_list(Atom).

conf_path(Req) ->
    <<"/api/v5", ?PREFIX, Path/binary>> = cowboy_req:path(Req),
    string:lexemes(Path, "/ ").

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
    with_default_value(#{type => boolean}, Conf);
gen_schema(Conf) when is_binary(Conf); is_atom(Conf) ->
    with_default_value(#{type => string}, Conf);
gen_schema(Conf) when is_number(Conf) ->
    with_default_value(#{type => number}, Conf);
gen_schema(Conf) when is_list(Conf) ->
    case io_lib:printable_unicode_list(Conf) of
        true ->
            gen_schema(unicode:characters_to_binary(Conf));
        false ->
            #{type => array, items => gen_schema(hd(Conf))}
    end;
gen_schema(Conf) when is_map(Conf) ->
    #{type => object, properties =>
    maps:map(fun(_K, V) -> gen_schema(V) end, Conf)};
gen_schema(_Conf) ->
    %% the conf is not of JSON supported type, it may have been converted
    %% by the hocon schema
    #{type => string}.

with_default_value(Type, Value) ->
    Type#{example => emqx_map_lib:binary_string(Value)}.
