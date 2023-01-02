%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_modules_api).

-import(minirest, [return/1]).

-rest_api(#{name   => list_all_modules,
            method => 'GET',
            path   => "/modules/",
            func   => list,
            descr  => "List all modules in the cluster"}).

-rest_api(#{name   => list_node_modules,
            method => 'GET',
            path   => "/nodes/:atom:node/modules/",
            func   => list,
            descr  => "List all modules on a node"}).

-rest_api(#{name   => load_node_module,
            method => 'PUT',
            path   => "/nodes/:atom:node/modules/:atom:module/load",
            func   => load,
            descr  => "Load a module"}).

-rest_api(#{name   => unload_node_module,
            method => 'PUT',
            path   => "/nodes/:atom:node/modules/:atom:module/unload",
            func   => unload,
            descr  => "Unload a module"}).

-rest_api(#{name   => reload_node_module,
            method => 'PUT',
            path   => "/nodes/:atom:node/modules/:atom:module/reload",
            func   => reload,
            descr  => "Reload a module"}).

-rest_api(#{name   => load_module,
            method => 'PUT',
            path   => "/modules/:atom:module/load",
            func   => load,
            descr  => "load a module in the cluster"}).

-rest_api(#{name   => unload_module,
            method => 'PUT',
            path   => "/modules/:atom:module/unload",
            func   => unload,
            descr  => "Unload a module in the cluster"}).

-rest_api(#{name   => reload_module,
            method => 'PUT',
            path   => "/modules/:atom:module/reload",
            func   => reload,
            descr  => "Reload a module in the cluster"}).

-export([ list/2
        , list_modules/1
        , load/2
        , unload/2
        , reload/2
        ]).

-export([ do_load_module/2
        , do_unload_module/2
        ]).

list(#{node := Node}, _Params) ->
    return({ok, [format(Module) || Module <- list_modules(Node)]});

list(_Bindings, _Params) ->
    return({ok, [format(Node, Modules) || {Node, Modules} <- list_modules()]}).

load(#{node := Node, module := Module}, _Params) ->
    return(do_load_module(Node, Module));

load(#{module := Module}, _Params) ->
    Results = [do_load_module(Node, Module) || Node <- ekka_mnesia:running_nodes()],
    case lists:filter(fun(Item) -> Item =/= ok end, Results) of
        [] ->
            return(ok);
        Errors ->
            return(lists:last(Errors))
    end.

unload(#{node := Node, module := Module}, _Params) ->
    return(do_unload_module(Node, Module));

unload(#{module := Module}, _Params) ->
    Results = [do_unload_module(Node, Module) || Node <- ekka_mnesia:running_nodes()],
    case lists:filter(fun(Item) -> Item =/= ok end, Results) of
        [] ->
            return(ok);
        Errors ->
            return(lists:last(Errors))
    end.

reload(#{node := Node, module := Module}, _Params) ->
    case reload_module(Node, Module) of
        ignore -> return(ok);
        Result -> return(Result)
    end;

reload(#{module := Module}, _Params) ->
    Results = [reload_module(Node, Module) || Node <- ekka_mnesia:running_nodes()],
    case lists:filter(fun(Item) -> Item =/= ok end, Results) of
        [] ->
            return(ok);
        Errors ->
            return(lists:last(Errors))
    end.

%%------------------------------------------------------------------------------
%% Internal Functions
%%------------------------------------------------------------------------------

format(Node, Modules) ->
    #{node => Node, modules => [format(Module) || Module <- Modules]}.

format({Name, Active}) ->
    #{name => Name,
      description => iolist_to_binary(Name:description()),
      active => Active}.

list_modules() ->
    [{Node, list_modules(Node)} || Node <- ekka_mnesia:running_nodes()].

list_modules(Node) when Node =:= node() ->
    emqx_modules:list();
list_modules(Node) ->
    rpc_call(Node, list_modules, [Node]).

do_load_module(Node, Module) when Node =:= node() ->
    emqx_modules:load(Module);
do_load_module(Node, Module) ->
    rpc_call(Node, do_load_module, [Node, Module]).

do_unload_module(Node, Module) when Node =:= node() ->
    emqx_modules:unload(Module);
do_unload_module(Node, Module) ->
    rpc_call(Node, do_unload_module, [Node, Module]).

reload_module(Node, Module) when Node =:= node() ->
    emqx_modules:reload(Module);
reload_module(Node, Module) ->
    rpc_call(Node, reload_module, [Node, Module]).

rpc_call(Node, Fun, Args) ->
    case rpc:call(Node, ?MODULE, Fun, Args) of
        {badrpc, Reason} -> {error, Reason};
        Res -> Res
    end.
