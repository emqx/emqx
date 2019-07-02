%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_endpoint).

-include("types.hrl").

%% APIs
-export([ new/0
        , new/1
        ]).

-export([ zone/1
        , client_id/1
        , mountpoint/1
        , is_superuser/1
        , credentials/1
        ]).

-export([update/2]).

-export([to_map/1]).

-export_type([endpoint/0]).

-opaque(endpoint() ::
        {endpoint,
         #{zone := emqx_types:zone(),
           peername := emqx_types:peername(),
           sockname => emqx_types:peername(),
           client_id := emqx_types:client_id(),
           username := emqx_types:username(),
           peercert := esockd_peercert:peercert(),
           is_superuser := boolean(),
           mountpoint := maybe(binary()),
           ws_cookie := maybe(list()),
           password => binary(),
           auth_result => emqx_types:auth_result(),
           anonymous => boolean(),
           atom() => term()
          }
        }).

-define(Endpoint(M), {endpoint, M}).

-define(Default, #{is_superuser => false,
                   anonymous => false
                  }).

-spec(new() -> endpoint()).
new() ->
    ?Endpoint(?Default).

-spec(new(map()) -> endpoint()).
new(M) when is_map(M) ->
    ?Endpoint(maps:merge(?Default, M)).

-spec(zone(endpoint()) -> emqx_zone:zone()).
zone(?Endpoint(#{zone := Zone})) ->
    Zone.

client_id(?Endpoint(#{client_id := ClientId})) ->
    ClientId.

-spec(mountpoint(endpoint()) -> maybe(binary())).
mountpoint(?Endpoint(#{mountpoint := Mountpoint})) ->
    Mountpoint;
mountpoint(_) -> undefined.

is_superuser(?Endpoint(#{is_superuser := B})) ->
    B.

update(Attrs, ?Endpoint(M)) ->
    ?Endpoint(maps:merge(M, Attrs)).

credentials(?Endpoint(M)) ->
    M. %% TODO: ...

-spec(to_map(endpoint()) -> map()).
to_map(?Endpoint(M)) ->
    M.

