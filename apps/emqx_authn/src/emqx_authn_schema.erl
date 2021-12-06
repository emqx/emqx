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

-include_lib("typerefl/include/types.hrl").

-export([ common_fields/0
        , roots/0
        , fields/1
        , authenticator_type/0
        , root_type/0
        , mechanism/1
        , backend/1
        ]).

roots() -> [].

fields(_) -> [].

common_fields() ->
    [ {enable, fun enable/1}
    ].

enable(type) -> boolean();
enable(default) -> true;
enable(desc) -> "Set to <code>false</code> to disable this auth provider";
enable(_) -> undefined.

authenticator_type() ->
    hoconsc:union(config_refs([Module || {_AuthnType, Module} <- emqx_authn:providers()])).

config_refs(Modules) ->
    lists:append([Module:refs() || Module <- Modules]).

%% authn is a core functionality however implemented outside fo emqx app
%% in emqx_schema, 'authentication' is a map() type which is to allow
%% EMQ X more plugable.
root_type() ->
    T = authenticator_type(),
    hoconsc:union([T, hoconsc:array(T)]).

mechanism(Name) ->
    hoconsc:mk(hoconsc:enum([Name]),
               #{nullable => false}).

backend(Name) ->
    hoconsc:mk(hoconsc:enum([Name]),
               #{nullable => false}).
