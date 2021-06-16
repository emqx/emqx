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
              , provider_name/0
              , provider_type/0
              ]).

-type(provider_name() :: binary()).
-type(provider_type() :: mnesia | jwt).

% structs() -> [chains].

structs() -> [""].

fields("") ->
    [ {chains, hoconsc:t({array, "chain"}, #{default => []})}
    ];

% fields(chains) ->
%     [fun chains/1];

fields(chain) ->
    [ {id, fun chain_id/1}
    , {providers, fun providers/1}
    ];

fields(provider) ->
    [ {name, fun provider_name/1}
    , {type, fun provider_type/1}
    , {config, fun config/1}
    ].

% chains(type) -> {array, "chain"};
% chains(default) -> [];
% chains(_) -> undefined.

chain_id(type) -> chain_id();
chain_id(_) -> undefined.

providers(type) -> {array, "provider"};
providers(default) -> [];
providers(_) -> undefined.

provider_name(type) -> provider_name();
provider_name(validator) -> [required(provider_name)];
provider_name(_) -> undefined.

provider_type(type) -> provider_type();
provider_type(_) -> undefined.

config(type) -> map();
config(_) -> undefined.

required(Opt) ->
    fun(undefined) -> {error, {missing_required, Opt}};
       (_) -> ok
    end.
