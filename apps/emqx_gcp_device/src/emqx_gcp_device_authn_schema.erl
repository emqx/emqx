%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_gcp_device_authn_schema).

-behaviour(emqx_authn_schema).

-export([
    namespace/0,
    fields/1,
    desc/1,
    refs/0,
    select_union_member/1
]).

-include("emqx_gcp_device.hrl").
-include_lib("hocon/include/hoconsc.hrl").

namespace() -> "authn".

refs() -> [?R_REF(gcp_device)].

select_union_member(#{<<"mechanism">> := ?AUTHN_MECHANISM_BIN}) ->
    refs();
select_union_member(_Value) ->
    undefined.

fields(gcp_device) ->
    [
        {mechanism, emqx_authn_schema:mechanism(gcp_device)}
    ] ++ emqx_authn_schema:common_fields().

desc(gcp_device) ->
    ?DESC(emqx_gcp_device_api, gcp_device);
desc(_) ->
    undefined.
