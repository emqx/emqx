%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mod_rewrite).

-behavior(emqx_gen_mod).

-include_lib("emqx.hrl").
-include_lib("emqx_mqtt.hrl").

%% APIs
-export([ rewrite_subscribe/3
        , rewrite_unsubscribe/3
        , rewrite_publish/2
        ]).

%% emqx_gen_mod callbacks
-export([ load/1
        , unload/1
        ]).

%%------------------------------------------------------------------------------
%% Load/Unload
%%------------------------------------------------------------------------------

load(RawRules) ->
    Rules = compile(RawRules),
    emqx_hooks:add('client.subscribe',   fun ?MODULE:rewrite_subscribe/3, [Rules]),
    emqx_hooks:add('client.unsubscribe', fun ?MODULE:rewrite_unsubscribe/3, [Rules]),
    emqx_hooks:add('message.publish',    fun ?MODULE:rewrite_publish/2, [Rules]).

rewrite_subscribe(_Credentials, TopicTable, Rules) ->
    {ok, [{match_rule(Topic, Rules), Opts} || {Topic, Opts} <- TopicTable]}.

rewrite_unsubscribe(_Credentials, TopicTable, Rules) ->
    {ok, [{match_rule(Topic, Rules), Opts} || {Topic, Opts} <- TopicTable]}.

rewrite_publish(Message = #message{topic = Topic}, Rules) ->
    {ok, Message#message{topic = match_rule(Topic, Rules)}}.

unload(_) ->
    emqx_hooks:del('client.subscribe',   fun ?MODULE:rewrite_subscribe/3),
    emqx_hooks:del('client.unsubscribe', fun ?MODULE:rewrite_unsubscribe/3),
    emqx_hooks:del('message.publish',    fun ?MODULE:rewrite_publish/2).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

match_rule(Topic, []) ->
    Topic;

match_rule(Topic, [{rewrite, Filter, MP, Dest} | Rules]) ->
    case emqx_topic:match(Topic, Filter) of
        true  -> match_regx(Topic, MP, Dest);
        false -> match_rule(Topic, Rules)
    end.

match_regx(Topic, MP, Dest) ->
    case re:run(Topic, MP, [{capture, all_but_first, list}]) of
        {match, Captured} ->
            Vars = lists:zip(["\\$" ++ integer_to_list(I)
                                || I <- lists:seq(1, length(Captured))], Captured),
            iolist_to_binary(lists:foldl(
                    fun({Var, Val}, Acc) ->
                        re:replace(Acc, Var, Val, [global])
                    end, Dest, Vars));
        nomatch -> Topic
    end.

compile(Rules) ->
    lists:map(fun({rewrite, Topic, Re, Dest}) ->
                  {ok, MP} = re:compile(Re),
                  {rewrite, Topic, MP, Dest}
              end, Rules).
