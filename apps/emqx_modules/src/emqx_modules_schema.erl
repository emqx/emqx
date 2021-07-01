%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_modules_schema).

-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).

-export([ structs/0
        , fields/1]).

structs() -> ["emqx_modules"].

fields("emqx_modules") ->
    [{modules, hoconsc:array(hoconsc:union([ hoconsc:ref(?MODULE, "common")
                                           , hoconsc:ref(?MODULE, "presence")
                                           , hoconsc:ref(?MODULE, "rewrite")
                                           , hoconsc:ref(?MODULE, "topic_metrics")
                                           ]))}];
fields("common") ->
    [ {type, hoconsc:enum([delayed, recon])}
    , {enable, emqx_schema:t(boolean(), undefined, false)}
    ];

fields("presence") ->
    [ {type, hoconsc:enum([presence])}
    , {enable, emqx_schema:t(boolean(), undefined, false)}
    , {qos, emqx_schema:t(integer(), undefined, 1)}
    ];
fields("rewrite") ->
    [ {type, hoconsc:enum([rewrite])}
    , {enable, emqx_schema:t(boolean(), undefined, false)}
    , {rules, hoconsc:array(hoconsc:ref(?MODULE, "rules"))}
    ];

fields("topic_metrics") ->
    [ {type, hoconsc:enum([topic_metrics])}
    , {enable, emqx_schema:t(boolean(), undefined, false)}
    , {topics, hoconsc:array(binary())}
    ];

fields("rules") ->
    [ {action, hoconsc:enum([publish, subscribe])}
    , {source_topic, emqx_schema:t(binary())}
    , {re, emqx_schema:t(binary())}
    , {dest_topic, emqx_schema:t(binary())}
    ].
