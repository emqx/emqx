%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mod_rewrite).

-behaviour(emqx_gen_mod).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

-ifdef(TEST).
-export([ compile/1
        , match_and_rewrite/2
        ]).
-endif.

%% APIs
-export([ rewrite_subscribe/4
        , rewrite_unsubscribe/4
        , rewrite_publish/2
        ]).

%% emqx_gen_mod callbacks
-export([ load/1
        , unload/1
        , description/0
        ]).

%%--------------------------------------------------------------------
%% Load/Unload
%%--------------------------------------------------------------------

load(RawRules) ->
    {PubRules, SubRules} = compile(RawRules),
    emqx_hooks:put('client.subscribe',   {?MODULE, rewrite_subscribe, [SubRules]}),
    emqx_hooks:put('client.unsubscribe', {?MODULE, rewrite_unsubscribe, [SubRules]}),
    emqx_hooks:put('message.publish',    {?MODULE, rewrite_publish, [PubRules]}).

rewrite_subscribe(_ClientInfo, _Properties, TopicFilters, Rules) ->
    {ok, [{match_and_rewrite(Topic, Rules), Opts} || {Topic, Opts} <- TopicFilters]}.

rewrite_unsubscribe(_ClientInfo, _Properties, TopicFilters, Rules) ->
    {ok, [{match_and_rewrite(Topic, Rules), Opts} || {Topic, Opts} <- TopicFilters]}.

rewrite_publish(Message = #message{topic = Topic}, Rules) ->
    {ok, Message#message{topic = match_and_rewrite(Topic, Rules)}}.

unload(_) ->
    emqx_hooks:del('client.subscribe',   {?MODULE, rewrite_subscribe}),
    emqx_hooks:del('client.unsubscribe', {?MODULE, rewrite_unsubscribe}),
    emqx_hooks:del('message.publish',    {?MODULE, rewrite_publish}).

description() ->
    "EMQ X Topic Rewrite Module".
%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

compile(Rules) ->
    PubRules = [ begin
                     {ok, MP} = re:compile(Re),
                     {rewrite, Topic, MP, Dest}
                 end || {rewrite, pub, Topic, Re, Dest}<- Rules ],
    SubRules = [ begin
                     {ok, MP} = re:compile(Re),
                     {rewrite, Topic, MP, Dest}
                 end || {rewrite, sub, Topic, Re, Dest}<- Rules ],
    {PubRules, SubRules}.

match_and_rewrite(Topic, []) ->
    Topic;

match_and_rewrite(Topic, [{rewrite, Filter, MP, Dest} | Rules]) ->
    case emqx_topic:match(Topic, Filter) of
        true  -> rewrite(Topic, MP, Dest);
        false -> match_and_rewrite(Topic, Rules)
    end.

rewrite(Topic, MP, Dest) ->
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

