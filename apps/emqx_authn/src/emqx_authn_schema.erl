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

-export([ structs/0
        , fields/1
        ]).

-export([ authenticator_name/1
        ]).

structs() -> [ "authentication" ].

fields("authentication") ->
    [ {enable, fun enable/1}
    , {authenticators, fun authenticators/1}
    ].

authenticator_name(type) -> binary();
authenticator_name(nullable) -> false;
authenticator_name(_) -> undefined.

enable(type) -> boolean();
enable(default) -> false;
enable(_) -> undefined.

authenticators(type) ->
    hoconsc:array({union, [ hoconsc:ref(emqx_authn_mnesia, config)
                          , hoconsc:ref(emqx_authn_mysql, config)
                          , hoconsc:ref(emqx_authn_pgsql, config)
                          , hoconsc:ref(emqx_authn_mongodb, standalone)
                          , hoconsc:ref(emqx_authn_mongodb, 'replica-set')
                          , hoconsc:ref(emqx_authn_mongodb, sharded)
                          , hoconsc:ref(emqx_authn_http, get)
                          , hoconsc:ref(emqx_authn_http, post)
                          , hoconsc:ref(emqx_authn_jwt, 'hmac-based')
                          , hoconsc:ref(emqx_authn_jwt, 'public-key')
                          , hoconsc:ref(emqx_authn_jwt, 'jwks')
                          , hoconsc:ref(emqx_enhanced_authn_scram_mnesia, config)
                          ]});
authenticators(default) -> [];
authenticators(_) -> undefined.
