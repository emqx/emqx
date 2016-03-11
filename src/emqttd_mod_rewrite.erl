%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

%% @doc emqttd rewrite module
-module(emqttd_mod_rewrite).

-behaviour(emqttd_gen_mod).

-include("emqttd.hrl").

-export([load/1, reload/1, unload/1]).

-export([rewrite_subscribe/3, rewrite_unsubscribe/3, rewrite_publish/2]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

load(Opts) ->
    File = proplists:get_value(file, Opts),
    {ok, Terms} = file:consult(File),
    Sections = compile(Terms),
    emqttd:hook('client.subscribe', fun ?MODULE:rewrite_subscribe/3, [Sections]),
    emqttd:hook('client.unsubscribe', fun ?MODULE:rewrite_unsubscribe/3, [Sections]),
    emqttd:hook('message.publish', fun ?MODULE:rewrite_publish/2, [Sections]).

rewrite_subscribe(_ClientId, TopicTable, Sections) ->
    lager:info("Rewrite subscribe: ~p", [TopicTable]),
    {ok, [{match_topic(Topic, Sections), Qos} || {Topic, Qos} <- TopicTable]}.

rewrite_unsubscribe(_ClientId, Topics, Sections) ->
    lager:info("Rewrite unsubscribe: ~p", [Topics]),
    {ok, [match_topic(Topic, Sections) || Topic <- Topics]}.

rewrite_publish(Message=#mqtt_message{topic = Topic}, Sections) ->
    %%TODO: this will not work if the client is always online.
    RewriteTopic =
    case get({rewrite, Topic}) of
        undefined ->
            DestTopic = match_topic(Topic, Sections),
            put({rewrite, Topic}, DestTopic), DestTopic;
        DestTopic ->
            DestTopic
        end,
    {ok, Message#mqtt_message{topic = RewriteTopic}}.

reload(File) ->
    %%TODO: The unload api is not right...
    case emqttd_app:is_mod_enabled(rewrite) of
        true -> 
            unload(state),
            load([{file, File}]);
        false ->
            {error, module_unloaded}
    end.
            
unload(_) ->
    emqttd:unhook('client.subscribe',  fun ?MODULE:rewrite_subscribe/3),
    emqttd:unhook('client.unsubscribe',fun ?MODULE:rewrite_unsubscribe/3),
    emqttd:unhook('message.publish',   fun ?MODULE:rewrite_publish/2).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

compile(Sections) ->
    C = fun({rewrite, Re, Dest}) ->
       {ok, MP} = re:compile(Re),
       {rewrite, MP, Dest}
    end,
    F = fun({topic, Topic, Rules}) ->
        {topic, list_to_binary(Topic), [C(R) || R <- Rules]}
    end,
    [F(Section) || Section <- Sections].

match_topic(Topic, []) ->
    Topic;
match_topic(Topic, [{topic, Filter, Rules} | Sections]) ->
    case emqttd_topic:match(Topic, Filter) of
        true ->
            match_rule(Topic, Rules);
        false ->
            match_topic(Topic, Sections)
    end.

match_rule(Topic, []) ->
    Topic;
match_rule(Topic, [{rewrite, MP, Dest} | Rules]) ->
    case re:run(Topic, MP, [{capture, all_but_first, list}]) of
        {match, Captured} ->
            Vars = lists:zip(["\\$" ++ integer_to_list(I)
                                || I <- lists:seq(1, length(Captured))], Captured),
            iolist_to_binary(lists:foldl(
                    fun({Var, Val}, Acc) ->
                            re:replace(Acc, Var, Val, [global])
                    end, Dest, Vars));
        nomatch ->
            match_rule(Topic, Rules)
    end.

