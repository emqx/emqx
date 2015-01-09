%%------------------------------------------------------------------------------
%% Copyright (c) 2012-2015, Feng Lee <feng@emqtt.io>
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

-module(emqtt_http).

-author('feng@emqtt.io').

-include("emqtt.hrl").

-import(proplists, [get_value/2, get_value/3]).

-export([handle/1]).

handle(Req) ->
	case authorized(Req) of
	true ->
		Path = Req:get(path),
		Method = Req:get(method),
		handle(Method, Path, Req);
	false ->
		error_logger:info_msg("Fobbidden"),
		Req:respond({401, [], <<"Fobbiden">>})
	end.

handle('POST', "/mqtt/publish", Req) ->
    Params = mochiweb_request:parse_post(Req),
	lager:info("~p~n", [Params]),
	Topic = list_to_binary(get_value("topic", Params)),
	Message = list_to_binary(get_value("message", Params)),
	emqtt_pubsub:publish(#mqtt_message {
				topic      = Topic,
				payload    = Message
	}),
	Req:ok({"text/plan", "ok"});

handle(_Method, _Path, Req) ->
	Req:not_found().

%%------------------------------------------------------------------------------
%% basic authorization
%%------------------------------------------------------------------------------
authorized(Req) ->
	case mochiweb_request:get_header_value("Authorization", Req) of
	undefined ->
		false;
	"Basic " ++ BasicAuth ->
		emqtt_auth:check(user_passwd(BasicAuth))
	end.

user_passwd(BasicAuth) ->
	list_to_tuple(binary:split(base64:decode(BasicAuth), <<":">>)). 

