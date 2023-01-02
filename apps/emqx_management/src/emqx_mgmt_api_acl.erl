%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mgmt_api_acl).

-include("emqx_mgmt.hrl").

-rest_api(#{name   => clean_acl_cache_all,
            method => 'DELETE',
            path   => "/acl-cache",
            func   => clean_all,
            descr  => "Clean acl cache on all nodes"}).

-rest_api(#{name   => clean_acl_cache_node,
            method => 'DELETE',
            path   => "nodes/:atom:node/acl-cache",
            func   => clean_node,
            descr  => "Clean acl cache on specific node"}).

-export([ clean_all/2
        , clean_node/2
        ]).

clean_all(_Bindings, _Params) ->
    case emqx_mgmt:clean_acl_cache_all() of
      ok -> minirest:return();
      {error, Reason} -> minirest:return({error, ?ERROR1, Reason})
    end.

clean_node(#{node := Node}, _Params) ->
    case emqx_mgmt:clean_acl_cache_all(Node) of
      ok -> minirest:return();
      {error, Reason} -> minirest:return({error, ?ERROR1, Reason})
    end.
