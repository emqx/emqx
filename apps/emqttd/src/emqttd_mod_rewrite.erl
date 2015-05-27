%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% emqttd rewrite module.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-module(emqttd_mod_rewrite).

-author("Feng Lee <feng@emqtt.io>").

-include_lib("emqtt/include/emqtt.hrl").

-behaviour(emqttd_gen_mod).

-export([load/1, reload/1, unload/1]).

-export([rewrite/2]).

%%%=============================================================================
%%% API
%%%=============================================================================

load(Opts) ->
    File = proplists:get_value(file, Opts),
    {ok, Terms} = file:consult(File),
    Sections = compile(Terms),
    emqttd_broker:hook(client_subscribe, {?MODULE, rewrite_subscribe}, 
                       {?MODULE, rewrite, [subscribe, Sections]}),
    emqttd_broker:hook(client_unsubscribe, {?MODULE, rewrite_unsubscribe},
                       {?MODULE, rewrite, [unsubscribe, Sections]}),
    emqttd_broker:hook(client_publish, {?MODULE, rewrite_publish},
                       {?MODULE, rewrite, [publish, Sections]}).

rewrite(TopicTable, [subscribe, Sections]) ->
    lager:info("rewrite subscribe: ~p", [TopicTable]),
    [{match_topic(Topic, Sections), Qos} || {Topic, Qos} <- TopicTable];

rewrite(Topics, [unsubscribe, Sections]) ->
    lager:info("rewrite unsubscribe: ~p", [Topics]),
    [match_topic(Topic, Sections) || Topic <- Topics];

rewrite(Message=#mqtt_message{topic = Topic}, [publish, Sections]) ->
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
    emqttd_broker:unhook(client_subscribe, {?MODULE, rewrite_subscribe}),
    emqttd_broker:unhook(client_unsubscribe, {?MODULE, rewrite_unsubscribe}),
    emqttd_broker:unhook(client_publish, {?MODULE, rewrite_publish}).

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

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
    case emqtt_topic:match(Topic, Filter) of
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
            %%TODO: stupid??? how to replace $1, $2?
            Vars = lists:zip(["\\$" ++ integer_to_list(I) || I <- lists:seq(1, length(Captured))], Captured),
            iolist_to_binary(lists:foldl(
                    fun({Var, Val}, Acc) ->
                            re:replace(Acc, Var, Val, [global])
                    end, Dest, Vars));
        nomatch ->
            match_rule(Topic, Rules)
    end.
