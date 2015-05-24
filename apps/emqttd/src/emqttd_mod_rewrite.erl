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

-behaviour(emqttd_gen_mod).

-export([load/1, reload/1, unload/1]).

-export([rewrite/2]).

%%%=============================================================================
%%% API
%%%=============================================================================

load(Opts) ->
    File = proplists:get_value(file, Opts),
    Sections = compile(file:consult(File)),
    emqttd_broker:hook(client_subscribe, {?MODULE, rewrite_subscribe}, 
                       {?MODULE, rewrite, [subscribe, Sections]}),
    emqttd_broker:hook(client_unsubscribe, {?MODULE, rewrite_unsubscribe},
                       {?MODULE, rewrite_unsubscribe, [unsubscribe, Sections]}),
    emqttd_broker:hook(client_publish, {?MODULE, rewrite_publish},
                       {?MODULE, rewrite_publish, [publish, Sections]}).

rewrite(TopicTable, [subscribe, _Sections]) ->
    lager:info("Rewrite Subscribe: ~p", [TopicTable]),
    TopicTable;

rewrite(Topics, [unsubscribe, _Sections]) ->
    lager:info("Rewrite Unsubscribe: ~p", [Topics]),
    Topics;

rewrite(Message, [publish, _Sections]) ->
    Message.

reload(File) ->
    %%TODO: The unload api is not right...
    unload(state), load([{file, File}]).
            
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
    [{topic, Topic, [C(R) || R <- Rules]} || {topic, Topic, Rules} <- Sections].

