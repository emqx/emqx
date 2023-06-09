%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
        array("rewrite", #{
            desc => "List of topic rewrite rules.",
            importance => ?IMPORTANCE_HIDDEN,
            validator => fun rewrite_validator/1,
            default => []
        }),
        array("topic_metrics", #{
            desc => "List of topics whose metrics are reported.",
            importance => ?IMPORTANCE_HIDDEN,
            default => []
        })
    ].

rewrite_validator(Rules) ->
    case
        lists:foldl(
            fun
                (#{<<"action">> := subscribe}, Acc) ->
                    Acc;
                (#{<<"dest_topic">> := DestTopic}, InvalidAcc) ->
                    try
                        true = emqx_topic:validate(name, DestTopic),
                        InvalidAcc
                    catch
                        _:_ ->
                            [DestTopic | InvalidAcc]
                    end
            end,
            [],
            Rules
        )
    of
        [] ->
            ok;
        InvalidTopics ->
            {
                error,
                #{
                    msg => "cannot_use_wildcard_for_destination_topic",
                    invalid_topics => InvalidTopics
                }
            }
    end.

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
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, #{
                regexp => Bin,
                compile_error => Reason
            }}
    end.

array(Name, Meta) -> {Name, ?HOCON(?ARRAY(?R_REF(Name)), Meta)}.
