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

-module(emqx_mod_rewrite).

-behaviour(emqx_gen_mod).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").

-ifdef(TEST).
-export([ compile/1
        , match_and_rewrite/3
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
    log_start(RawRules),
    emqx_hooks:put('client.subscribe',   {?MODULE, rewrite_subscribe, [SubRules]}, 1000),
    emqx_hooks:put('client.unsubscribe', {?MODULE, rewrite_unsubscribe, [SubRules]}, 1000),
    emqx_hooks:put('message.publish',    {?MODULE, rewrite_publish, [PubRules]}, 1000).

rewrite_subscribe(ClientInfo, _Properties, TopicFilters, Rules) ->
    Binds = fill_client_binds(ClientInfo),
    {ok, [{match_and_rewrite(Topic, Rules, Binds), Opts} || {Topic, Opts} <- TopicFilters]}.

rewrite_unsubscribe(ClientInfo, _Properties, TopicFilters, Rules) ->
    Binds = fill_client_binds(ClientInfo),
    {ok, [{match_and_rewrite(Topic, Rules, Binds), Opts} || {Topic, Opts} <- TopicFilters]}.

rewrite_publish(Message = #message{topic = Topic}, Rules) ->
    Binds = fill_client_binds(Message),
    {ok, Message#message{topic = match_and_rewrite(Topic, Rules, Binds)}}.

unload(_) ->
    ?LOG(info, "[Rewrite] Unload"),
    emqx_hooks:del('client.subscribe',   {?MODULE, rewrite_subscribe}),
    emqx_hooks:del('client.unsubscribe', {?MODULE, rewrite_unsubscribe}),
    emqx_hooks:del('message.publish',    {?MODULE, rewrite_publish}).

description() ->
    "EMQ X Topic Rewrite Module".
%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

log_start(Rules) ->
    PubRules = [{pub, Topic, Re, Dest} || {rewrite, pub, Topic, Re, Dest} <- Rules],
    SubRules = [{sub, Topic, Re, Dest} || {rewrite, sub, Topic, Re, Dest} <- Rules],
    ?LOG(info, "[Rewrite] Load: pub rules count ~p sub rules count ~p",
        [erlang:length(PubRules), erlang:length(SubRules)]),
    log_rule(PubRules, 1),
    log_rule(SubRules, 1).

log_rule([], _Index) -> ok;
log_rule([{Type, Topic, Re, Dest} | Rules], Index) ->
    ?LOG(info, "[Rewrite] Load ~p rule[~p]: source: ~ts, re: ~ts, dest: ~ts",
        [Type, Index, Topic, Re, Dest]),
    log_rule(Rules, Index + 1).

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

match_and_rewrite(Topic, [], _) ->
    Topic;

match_and_rewrite(Topic, [{rewrite, Filter, MP, Dest} | Rules], Binds) ->
    case emqx_topic:match(Topic, Filter) of
        true  -> rewrite(Topic, MP, Dest, Binds);
        false -> match_and_rewrite(Topic, Rules, Binds)
    end.

rewrite(Topic, MP, Dest, Binds) ->
    NewTopic =
        case re:run(Topic, MP, [{capture, all_but_first, list}]) of
            {match, Captured} ->
                Vars = lists:zip(["\\$" ++ integer_to_list(I)
                                    || I <- lists:seq(1, length(Captured))], Captured),
                iolist_to_binary(lists:foldl(
                        fun({Var, Val}, Acc) ->
                            re:replace(Acc, Var, Val, [global])
                        end, Dest, Binds ++ Vars));
            nomatch -> Topic
        end,
    ?LOG(debug, "[Rewrite] topic ~0p, params: ~0p dest topic: ~p", [Topic, Binds, NewTopic]),
    NewTopic.

fill_client_binds(#{clientid := ClientId, username := Username}) ->
    filter_client_binds([{"%c", ClientId}, {"%u", Username}]);

fill_client_binds(#message{from = ClientId, headers = Headers}) ->
    Username = maps:get(username, Headers, undefined),
    filter_client_binds([{"%c", ClientId}, {"%u", Username}]).

filter_client_binds(Binds) ->
    lists:filter(fun({_, undefined}) -> false;
                    ({_, <<"">>}) -> false;
                    ({_, ""}) -> false;
                    (_) -> true
                   end,
                 Binds).
