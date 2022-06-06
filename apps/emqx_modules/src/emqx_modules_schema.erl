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

-include_lib("hocon/include/hoconsc.hrl").
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
        array("rewrite", #{desc => "List of topic rewrite rules."}),
        array("topic_metrics", #{desc => "List of topics whose metrics are reported."})
    ].

fields("telemetry") ->
    [{enable, ?HOCON(boolean(), #{default => true, desc => "Enable telemetry."})}];
fields("delayed") ->
    [
        {enable, ?HOCON(boolean(), #{default => true, desc => ?DESC(enable)})},
        {max_delayed_messages,
            ?HOCON(integer(), #{desc => ?DESC(max_delayed_messages), default => 0})}
    ];
fields("rewrite") ->
    [
        {action,
            ?HOCON(
                hoconsc:enum([subscribe, publish, all]),
                #{required => true, desc => ?DESC(tr_action), example => publish}
            )},
        {source_topic,
            ?HOCON(
                binary(),
                #{required => true, desc => ?DESC(tr_source_topic), example => "x/#"}
            )},
        {dest_topic,
            ?HOCON(
                binary(),
                #{required => true, desc => ?DESC(tr_dest_topic), example => "z/y/$1"}
            )},
        {re, fun regular_expression/1}
    ];
fields("topic_metrics") ->
    [{topic, ?HOCON(binary(), #{desc => "Collect metrics for the topic."})}].

desc("telemetry") ->
    "Settings for the telemetry module.";
desc("delayed") ->
    "Settings for the delayed module.";
desc("rewrite") ->
    ?DESC(rewrite);
desc("topic_metrics") ->
    "";
desc(_) ->
    undefined.

regular_expression(type) -> binary();
regular_expression(required) -> true;
regular_expression(desc) -> ?DESC(tr_re);
regular_expression(example) -> "^x/y/(.+)$";
regular_expression(validator) -> fun is_re/1;
regular_expression(_) -> undefined.

is_re(Bin) ->
    case re:compile(Bin) of
        {ok, _} -> ok;
        {error, Reason} -> {error, {Bin, Reason}}
    end.

array(Name, Meta) -> {Name, ?HOCON(?ARRAY(?R_REF(Name)), Meta)}.
