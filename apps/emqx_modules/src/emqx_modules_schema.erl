%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

namespace() -> modules.

roots() ->
    [
        "delayed",
        "telemetry",
        array("rewrite"),
        array("topic_metrics")
    ].

fields("telemetry") ->
    [{enable, hoconsc:mk(boolean(), #{default => false})}];
fields("delayed") ->
    [
        {enable, hoconsc:mk(boolean(), #{default => false})},
        {max_delayed_messages, sc(integer(), #{})}
    ];
fields("rewrite") ->
    [
        {action,
            sc(
                hoconsc:enum([subscribe, publish, all]),
                #{desc => <<"Action">>, example => publish}
            )},
        {source_topic,
            sc(
                binary(),
                #{desc => <<"Origin Topic">>, example => "x/#"}
            )},
        {dest_topic,
            sc(
                binary(),
                #{desc => <<"Destination Topic">>, example => "z/y/$1"}
            )},
        {re, fun regular_expression/1}
    ];
fields("topic_metrics") ->
    [{topic, sc(binary(), #{})}].

desc("telemetry") ->
    "Settings for the telemetry module.";
desc("delayed") ->
    "Settings for the delayed module.";
desc("rewrite") ->
    "Settings for the rewrite module.";
desc("topic_metrics") ->
    "Settings for the topic metrics module.";
desc(_) ->
    undefined.

regular_expression(type) -> binary();
regular_expression(desc) -> "Regular expressions";
regular_expression(example) -> "^x/y/(.+)$";
regular_expression(validator) -> fun is_re/1;
regular_expression(_) -> undefined.

is_re(Bin) ->
    case re:compile(Bin) of
        {ok, _} -> ok;
        {error, Reason} -> {error, {Bin, Reason}}
    end.

array(Name) -> {Name, hoconsc:array(hoconsc:ref(?MODULE, Name))}.

sc(Type, Meta) -> hoconsc:mk(Type, Meta).
