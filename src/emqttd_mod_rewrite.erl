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

-export([rewrite/3, rewrite/4]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

load(Opts) ->
    File = proplists:get_value(file, Opts),
    {ok, Terms} = file:consult(File),
    Sections = compile(Terms),
    emqttd_broker:hook('client.subscribe', {?MODULE, rewrite_subscribe}, 
                        {?MODULE, rewrite, [subscribe, Sections]}),
    emqttd_broker:hook('client.unsubscribe', {?MODULE, rewrite_unsubscribe},
                        {?MODULE, rewrite, [unsubscribe, Sections]}),
    emqttd_broker:hook('message.publish', {?MODULE, rewrite_publish},
                        {?MODULE, rewrite, [publish, Sections]}),
    ok.

rewrite(_ClientId, TopicTable, subscribe, Sections) ->
    lager:info("rewrite subscribe: ~p", [TopicTable]),
    [{match_topic(Topic, Sections), Qos} || {Topic, Qos} <- TopicTable];

rewrite(_ClientId, Topics, unsubscribe, Sections) ->
    lager:info("rewrite unsubscribe: ~p", [Topics]),
    [match_topic(Topic, Sections) || Topic <- Topics].

rewrite(Message=#mqtt_message{topic = Topic}, publish, Sections) ->
    %%TODO: this will not work if the client is always online.
    RewriteTopic =
    case get({rewrite, Topic}) of
        undefined ->
            DestTopic = match_topic(Topic, Sections),
            put({rewrite, Topic}, DestTopic), DestTopic;
        DestTopic ->
            DestTopic
        end,
    Message#mqtt_message{topic = RewriteTopic}.

reload(File) ->
    %%TODO: The unload api is not right...
    case emqttd:is_mod_enabled(rewrite) of
        true -> 
            unload(state),
            load([{file, File}]);
        false ->
            {error, module_unloaded}
    end.
            
unload(_) ->
    emqttd_broker:unhook('client.subscribe',  {?MODULE, rewrite_subscribe}),
    emqttd_broker:unhook('client.unsubscribe',{?MODULE, rewrite_unsubscribe}),
    emqttd_broker:unhook('message.publish',   {?MODULE, rewrite_publish}).

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

