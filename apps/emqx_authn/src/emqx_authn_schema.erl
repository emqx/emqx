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
              , algorithm/0
              ]).

structs() -> [authn].

fields(authn) ->
    [{chains, fun chains/1}];

fields("chain") ->
    [ {id, fun chain_id/1}
    , {services, hoconsc:array({union, ["mnesia", "jwt"]})}
    ];

fields("mnesia") ->
    [ {name, fun service_name/1}
    , {type, type(mnesia)}
    , {config, config(mnesia)}
    ];

fields("jwt") ->
    [ {name, fun service_name/1}
    , {type, type(jwt)}
    , {config, config(jwt)}
    ];

fields("mneisa_config") ->
    [ {user_id_type, fun user_id_type/1}
    , {password_hash_algorithm, fun password_hash_algorithm/1}
    , {salt_rounds, fun salt_rounds/1}
    ].

fields("jwt_config") ->
    [ {use_jwks,               fun use_jwks/1}
    , {jwks_endpoint,          fun jwks_endpoint/1}
    , {refresh_interval,       fun refresh_interval/1}
    , {algorithm,              fun algorithm/1}
    , {secret,                 fun secret/1}
    , {secret_base64_encoded,  fun secret_base64_encoded/1}
    , {jwt_certfile,           fun jwt_certfile/1}
    , {cacertfile,             fun cacertfile/1}
    , {certfile,               fun certfile/1}
    , {keyfile,                fun keyfile/1}
    , {verify,                 fun verify/1}
    , {server_name_indication, fun server_name_indication/1}
    ].

chains(type) -> {array, "chain"};
chains(default) -> [];
chains(_) -> undefined.

chain_id(type) -> chain_id();
chain_id(_) -> undefined.

service_name(type) -> service_name();
service_name(validator) -> [required(service_name)];
service_name(_) -> undefined.

type(Type) ->
    fun(type) -> {enum, [Type]};
       (_) -> undefined
    end.

config(Type) ->
    fun(type) ->
        case Type of
            mnesia -> "mnesia_config";
            jwt -> "jwt_config"
        end;
       (_) -> undefined
    end.

%%------------------------------------------------------------------------------
%% Mnesia AuthN Provider Fields
%%------------------------------------------------------------------------------

user_id_type(type) -> user_id_type();
user_id_type(default) -> clientid;
user_id_type(_) -> undefined.

password_hash_algorithm(type) -> password_hash_algorithm();
password_hash_algorithm(default) -> sha256;
password_hash_algorithm(_) -> undefined.

salt_rounds(type) -> integer();
salt_rounds(default) -> 10;
salt_rounds(_) -> undefined.

%%------------------------------------------------------------------------------
%% JWT AuthN Provider Fields
%%------------------------------------------------------------------------------

use_jwks(type) -> boolean();
use_jwks(default) -> false;
use_jwks(_) -> undefined.

jwks_endpoint(type) -> string();
jwks_endpoint(_) -> undefined.

refresh_interval(type) -> integer();
refresh_interval(default) -> 300;
refresh_interval(_) -> undefined.

algorithm(type) -> algorithm();
algorithm(default) -> 'hmac-based';
algorithm(_) -> undefined.

secret(type) -> string();
secret(_) -> undefined.

secret_base64_encoded(type) -> boolean();
secret_base64_encoded(defualt) -> false;
secret_base64_encoded(_) -> undefined.

jwt_certfile(type) -> string();
jwt_certfile(_) -> undefined.

cacertfile(type) -> string();
cacertfile(_) -> undefined.

certfile(type) -> string();
certfile(_) -> undefined.

keyfile(type) -> string();
keyfile(_) -> undefined.

verify(type) -> boolean();
verify(default) -> false;
verify(_) -> undefined.

server_name_indication(type) -> string();
server_name_indication(_) -> undefined.

required(Opt) ->
    fun(undefined) -> {error, {missing_required, Opt}};
       (_) -> ok
    end.
