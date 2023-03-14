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

-module(emqx_mod_rewrite).

-behaviour(emqx_gen_mod).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").

-ifdef(TEST).
-export([ compile_rules/1
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

-type(topic() :: binary()).

%%--------------------------------------------------------------------
%% Load/Unload
%%--------------------------------------------------------------------

load(RawRules) ->
    {PubRules, SubRules} = compile_rules(RawRules),
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
    "EMQX Topic Rewrite Module".
%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

compile_rules(RawRules) ->
    compile(validate_rules(RawRules)).

compile({PubRules, SubRules}) ->
    CompileRE =
        fun({rewrite, RewriteFrom, Re, RewriteTo}) ->
                {ok, MP} = re:compile(Re),
                {rewrite, RewriteFrom, MP, RewriteTo}
        end,
    {lists:map(CompileRE, PubRules), lists:map(CompileRE, SubRules)}.

validate_rules(Rules) ->
    PubRules = [{rewrite, RewriteFrom, Re, RewriteTo} ||
                   {rewrite, pub, RewriteFrom, Re, RewriteTo} <- Rules,
                   validate_rule(pub, RewriteFrom, RewriteTo)
               ],
    SubRules = [{rewrite, RewriteFrom, Re, RewriteTo} ||
                   {rewrite, sub, RewriteFrom, Re, RewriteTo} <- Rules,
                   validate_rule(sub, RewriteFrom, RewriteTo)
               ],
    ?LOG(info, "[Rewrite] Load: pub rules count ~p sub rules count ~p",
        [erlang:length(PubRules), erlang:length(SubRules)]),
    log_rules(pub, PubRules),
    log_rules(sub, SubRules),
    {PubRules, SubRules}.

validate_rule(Type, RewriteFrom, RewriteTo) ->
    case validate_topic(filter, RewriteFrom) of
        ok ->
            case validate_topic(dest_topic_type(Type), RewriteTo) of
                ok ->
                    true;
                {error, Reason} ->
                    log_invalid_rule(to, Type, RewriteTo, Reason),
                    false
            end;
        {error, Reason} ->
            log_invalid_rule(from, Type, RewriteFrom, Reason),
            false
    end.

log_invalid_rule(Direction, Type, Topic, Reason) ->
    ?LOG(warning, "Invalid rewrite ~p rule for rewrite ~p topic '~ts' discarded. Reason: ~p",
         [type_to_name(Type), Direction, Topic, Reason]).

log_rules(Type, Rules) ->
    do_log_rules(Type, Rules, 1).

do_log_rules(_Type, [], _Index) -> ok;
do_log_rules(Type, [{_, Topic, Re, Dest} | Rules], Index) ->
    ?LOG(info, "[Rewrite] Load ~p rule[~p]: source: ~ts, re: ~ts, dest: ~ts",
        [Type, Index, Topic, Re, Dest]),
    do_log_rules(Type, Rules, Index + 1).

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
    filter_client_binds([{"%c", bin(ClientId)}, {"%u", bin(Username)}]);

fill_client_binds(#message{from = ClientId, headers = Headers}) ->
    Username = maps:get(username, Headers, undefined),
    filter_client_binds([{"%c", bin(ClientId)}, {"%u", bin(Username)}]).

filter_client_binds(Binds) ->
    lists:filter(fun({_, undefined}) -> false;
                    ({_, <<"">>}) -> false;
                    ({_, ""}) -> false;
                    (_) -> true
                   end,
                 Binds).

type_to_name(pub) -> 'PUBLISH';
type_to_name(sub) -> 'SUBSCRIBE'.

dest_topic_type(pub) -> name;
dest_topic_type(sub) -> filter.

-spec(validate_topic(name | filter, topic()) -> ok | {error, term()}).
validate_topic(Type, Topic) ->
    try
        true = emqx_topic:validate(Type, Topic),
        ok
    catch
        error:Reason ->
            {error, Reason}
    end.

bin(S) when is_binary(S) -> S;
bin(S) when is_list(S) -> list_to_binary(S);
bin(S) when is_atom(S) -> atom_to_binary(S, utf8).
