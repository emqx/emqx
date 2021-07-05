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
-module(emqx_mgmt_api_plugins).

-rest_api(v1).

-include_lib("emqx/include/emqx.hrl").

-define(PLUGIN_NOT_FOUND, <<"{\"code\": \"RESOURCE_NOT_FOUND\", \"reason\": \"Plugin name not found\"}">>).

%% API
-export([ rest_schema/0
        , rest_api/0]).

-export([ handle_list/1
        , handle_load/1
        , handle_unload/1
        , handle_reload/1]).

rest_schema() ->
    DefinitionName = <<"plugin">>,
    DefinitionProperties = #{
        <<"node">> => #{
            type => <<"string">>,
            description => <<"Node name">>},
        <<"name">> => #{
            type => <<"string">>,
            description => <<"Plugin name">>},
        <<"verion">> => #{
            type => <<"string">>,
            description => <<"Plugin verion">>},
        <<"decription">> => #{
            type => <<"string">>,
            description => <<"Plugin decription">>},
        <<"active">> => #{
            type => <<"boolean">>,
            description => <<"Whether the plugin is active">>}},
    [{DefinitionName, DefinitionProperties}].

rest_api() ->
    [ plugins_api()
    , load_plugin_api()
    , unload_plugin_api()
    , reload_plugin_api()].

plugins_api() ->
    Metadata = #{
        get => #{
            tags => ["system"],
            description => "EMQ X plugins",
            operationId => handle_list,
            responses => #{
                <<"200">> => #{
                    schema => #{
                        type => array,
                        items => cowboy_swagger:schema(<<"plugin">>)}}}}},
    {"/plugins", Metadata}.

load_plugin_api() ->
    Metadata = plugin_api_metadata("load plugin", handle_load),
    {"/plugins/:name/load", Metadata}.

unload_plugin_api() ->
    Metadata = plugin_api_metadata("unload plugin", handle_unload),
    {"/plugins/:name/unload", Metadata}.

reload_plugin_api() ->
    Metadata = plugin_api_metadata("reload plugin", handle_reload),
    {"/plugins/:name/reload", Metadata}.

plugin_api_metadata(Desc, OperationId) ->
    #{
        put => #{
            tags => ["system"],
            description => Desc,
            operationId => OperationId,
            parameters => [#{
                name => name,
                in => path,
                required => true,
                description => <<"Plugin name">>,
                type => string}],
            responses => #{
                <<"404">> => emqx_mgmt_util:not_found_schema(<<"Plugin name not found">>),
                <<"200">> => #{description => <<"operation ok">>}}}}.

%%%==============================================================================================
%% parameters trans
handle_list(_Request) ->
    list(#{}).

handle_load(Request) ->
    Name = cowboy_req:binding(name, Request),
    load(Name).

handle_unload(Request) ->
    Name = cowboy_req:binding(name, Request),
    unload(Name).

handle_reload(Request) ->
    Name = cowboy_req:binding(name, Request),
    reload(Name).


%%%==============================================================================================
%% api apply
list(_) ->
    Result = lists:append([format(Plugins) || {_, Plugins} <- emqx_mgmt:list_plugins()]),
    Response = emqx_json:encode(Result),
    {ok, Response}.

load(Name) ->
    Plugin = binary_to_atom(Name, utf8),
    case do_load(Plugin, ekka_mnesia:running_nodes()) of
        ok ->
            {ok};
        {error, not_found} ->
            {404, ?PLUGIN_NOT_FOUND};
        {error, Reason} ->
            Response = emqx_json:encode(#{code => "UN_KNOW_ERROR", reason => io_lib:format("~p", [Reason])}),
            do_unload(Plugin, ekka_mnesia:running_nodes()),
            {500, Response}
    end.

unload(Name) ->
    Plugin = binary_to_atom(Name, utf8),
    case do_unload(Plugin, ekka_mnesia:running_nodes()) of
        ok ->
            {ok};
        {error, Reason} ->
            Response = emqx_json:encode(#{code => "UN_KNOW_ERROR", reason => io_lib:format("~p", [Reason])}),
            {500, Response}
    end.

reload(Name) ->
    Plugin = binary_to_atom(Name, utf8),
    case do_reload(Plugin, ekka_mnesia:running_nodes()) of
        ok ->
            {ok};
        {error, not_found} ->
            {404, ?PLUGIN_NOT_FOUND};
        {error, Reason} ->
            Response = emqx_json:encode(#{code => "UN_KNOW_ERROR", reason => list_to_binary(io_lib:format("~p", [Reason]))}),
            {500, Response}
    end.

%%%==============================================================================================
%% internal
format(Plugins) when is_list(Plugins)->
    [format(Plugin) || Plugin <- Plugins];
format(#plugin{name = Name, descr = Descr, active = Active}) ->
    #{
        node => node(),
        name => Name,
        description => list_to_binary(Descr),
        active => Active
    }.

do_load(_Plugin, []) ->
    ok;
do_load(Plugin, [Node | Nodes]) ->
    case emqx_mgmt:load_plugin(Node, Plugin) of
        ok ->
            do_load(Plugin, Nodes);
        {error, already_started} ->
            do_load(Plugin, Nodes);
        {error, not_found} ->
            {error, not_found};
        {error, Reason} ->
            {error, {Node, Reason}}
    end.

do_unload(_Plugin, []) ->
    ok;
do_unload(Plugin, [Node | Nodes]) ->
    case emqx_mgmt:unload_plugin(Node, Plugin) of
        ok ->
            do_unload(Plugin, Nodes);
        {error, not_found} ->
            do_unload(Plugin, Nodes);
        {error, not_started} ->
            do_unload(Plugin, Nodes);
        {error, Reason} ->
            {error, {Node, Reason}}
    end.

do_reload(_Plugin, []) ->
    ok;
do_reload(Plugin, [Node | Nodes]) ->
    case emqx_mgmt:reload_plugin(Node, Plugin) of
        ok ->
            do_reload(Plugin, Nodes);
        Error ->
            Error
    end.

