%%--------------------------------------------------------------------
%% Copyright (c) 2017-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exhook_schema).

-dialyzer(no_return).
-dialyzer(no_match).
-dialyzer(no_contracts).
-dialyzer(no_unused).
-dialyzer(no_fail_call).

-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).

-type duration() :: integer().

-typerefl_from_string({duration/0, emqx_schema, to_duration}).

-reflect_type([duration/0]).

-export([roots/0, fields/1]).

-export([t/1, t/3, t/4, ref/1]).

roots() -> [exhook].

fields(exhook) ->
    [ {request_failed_action, t(union([deny, ignore]), undefined, deny)}
    , {request_timeout, t(duration(), undefined, "5s")}
    , {auto_reconnect, t(union([false, duration()]), undefined, "60s")}
    , {servers, t(hoconsc:array(ref(servers)), undefined, [])}
    ];

fields(servers) ->
    [ {name, string()}
    , {url, string()}
    , {ssl, t(ref(ssl_conf_group))}
    ];

fields(ssl_conf_group) ->
    [ {cacertfile, string()}
    , {certfile, string()}
    , {keyfile, string()}
    ].

%% types

t(Type) -> #{type => Type}.

t(Type, Mapping, Default) ->
    hoconsc:t(Type, #{mapping => Mapping, default => Default}).

t(Type, Mapping, Default, OverrideEnv) ->
    hoconsc:t(Type, #{ mapping => Mapping
                     , default => Default
                     , override_env => OverrideEnv
                     }).

ref(Field) ->
    hoconsc:ref(?MODULE, Field).
