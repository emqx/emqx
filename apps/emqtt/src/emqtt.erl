%%-----------------------------------------------------------------------------
%% Copyright (c) 2014, Feng Lee <feng.lee@slimchat.io>
%% 
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%% 
%% The above copyright notice and this permission notice shall be included in all
%% copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%% SOFTWARE.
%%------------------------------------------------------------------------------

-module(emqtt).

-export([listen/1]).

-define(MQTT_SOCKOPTS, [
	binary,
	{packet,        raw},
	{reuseaddr,     true},
	{backlog,       512},
	{nodelay,       false}
]).

listen(Listeners) when is_list(Listeners) ->
    [listen(Listener) || Listener <- Listeners];

listen({mqtt, Port, Options}) ->
    MFArgs = {emqtt_client, start_link, []},
    esockd:listen(mqtt, Port, Options ++ ?MQTT_SOCKOPTS, MFArgs);

listen({http, Port, Options}) ->
	Auth = proplists:get_value(auth, Options),
    MFArgs = {emqtt_http, handle, [Auth]},
	mochiweb:start_http(Port, proplists:delete(auth, Options), MFArgs).
    
