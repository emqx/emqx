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
              , authenticator_name/0
              ]).

structs() -> [authn].

fields(authn) ->
    [ {chains, fun chains/1}
    , {bindings, fun bindings/1}];

fields('simple-chain') ->
    [ {id, fun chain_id/1}
    , {type, {enum, [simple]}}
    , {authenticators, fun simple_authenticators/1}
    ];

% fields('enhanced-chain') ->
%     [ {id, fun chain_id/1}
%     , {type, {enum, [enhanced]}}
%     , {authenticators, fun enhanced_authenticators/1}
%     ];

fields(binding) ->
    [ {chain_id, fun chain_id/1}
    , {listeners, fun listeners/1}
    ];

fields('built-in-database') ->
    [ {name, fun authenticator_name/1}
    , {type, {enum, ['built-in-database']}}
    , {config, hoconsc:t(hoconsc:ref(emqx_authn_mnesia, config))}
    ];

% fields('enhanced-built-in-database') ->
%     [ {name, fun authenticator_name/1}
%     , {type, {enum, ['built-in-database']}}
%     , {config, hoconsc:t(hoconsc:ref(emqx_enhanced_authn_mnesia, config))}
%     ];

fields(jwt) ->
    [ {name, fun authenticator_name/1}
    , {type, {enum, [jwt]}}
    , {config, hoconsc:t(hoconsc:ref(emqx_authn_jwt, config))}
    ];

fields(mysql) ->
    [ {name, fun authenticator_name/1}
    , {type, {enum, [mysql]}}
    , {config, hoconsc:t(hoconsc:ref(emqx_authn_mysql, config))}
    ];

fields(pgsql) ->
    [ {name, fun authenticator_name/1}
    , {type, {enum, [postgresql]}}
    , {config, hoconsc:t(hoconsc:ref(emqx_authn_pgsql, config))}
    ].

chains(type) -> hoconsc:array({union, [hoconsc:ref('simple-chain')]});
chains(default) -> [];
chains(_) -> undefined.

chain_id(type) -> chain_id();
chain_id(nullable) -> false;
chain_id(_) -> undefined.

simple_authenticators(type) ->
    hoconsc:array({union, [ hoconsc:ref('built-in-database')
                          , hoconsc:ref(jwt)
                          , hoconsc:ref(mysql)
                          , hoconsc:ref(pgsql)]});
simple_authenticators(default) -> [];
simple_authenticators(_) -> undefined.

% enhanced_authenticators(type) ->
%     hoconsc:array({union, [hoconsc:ref('enhanced-built-in-database')]});
% enhanced_authenticators(default) -> [];
% enhanced_authenticators(_) -> undefined.

authenticator_name(type) -> authenticator_name();
authenticator_name(nullable) -> false;
authenticator_name(_) -> undefined.

bindings(type) -> hoconsc:array(hoconsc:ref(binding));
bindings(default) -> [];
bindings(_) -> undefined.

listeners(type) -> hoconsc:array(binary());
listeners(default) -> [];
listeners(_) -> undefined.
