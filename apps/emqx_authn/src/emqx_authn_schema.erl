%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_schema).

-include("emqx_authn.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).

-export([structs/0, fields/1]).

-reflect_type([ chain_id/0
              , service_name/0
              ]).

structs() -> [authn].

fields(authn) ->
    [{chains, fun chains/1}];

fields(chain) ->
    [ {id, fun chain_id/1}
    , {services, fun services/1}
    ];

fields(mnesia) ->
    [ {name, fun service_name/1}
    , {type, {enum, [mnesia]}}
    , {config, hoconsc:t(hoconsc:ref(emqx_authn_mnesia, config))}
    ];

fields(jwt) ->
    [ {name, fun service_name/1}
    , {type, {enum, [jwt]}}
    , {config, hoconsc:t(hoconsc:ref(emqx_authn_jwt, config))}
    ];

fields(mysql) ->
    [ {name, fun service_name/1}
    , {type, {enum, [mysql]}}
    , {config, hoconsc:t(hoconsc:ref(emqx_authn_mysql, config))}
    ];

fields(pgsql) ->
    [ {name, fun service_name/1}
    , {type, {enum, [postgresql]}}
    , {config, hoconsc:t(hoconsc:ref(emqx_authn_pgsql, config))}
    ].

chains(type) -> hoconsc:array(hoconsc:ref(chain));
chains(default) -> [];
chains(_) -> undefined.

chain_id(type) -> chain_id();
chain_id(nullable) -> false;
chain_id(_) -> undefined.

services(type) -> hoconsc:array({union, [ hoconsc:ref(mnesia)
                                        , hoconsc:ref(jwt)
                                        , hoconsc:ref(mysql)
                                        , hoconsc:ref(pgsql)]});
services(default) -> [];
services(_) -> undefined.

service_name(type) -> service_name();
service_name(nullable) -> false;
service_name(_) -> undefined.
