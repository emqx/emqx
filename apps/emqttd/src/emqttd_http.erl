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
%%% emqttd http publish API and websocket client.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_http).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-include_lib("emqtt/include/emqtt.hrl").

-import(proplists, [get_value/2, get_value/3]).

-export([handle_request/1]).

handle_request(Req) ->
    handle_request(Req:get(method), Req:get(path), Req).

%%------------------------------------------------------------------------------
%% HTTP Publish API
%%------------------------------------------------------------------------------
handle_request('POST', "/mqtt/publish", Req) ->
    Params = mochiweb_request:parse_post(Req),
    lager:info("HTTP Publish: ~p", [Params]),
	case authorized(Req) of
	true ->
        Qos = int(get_value("qos", Params, "0")),
        Retain = bool(get_value("retain", Params,  "0")),
        Topic = list_to_binary(get_value("topic", Params)),
        Message = list_to_binary(get_value("message", Params)),
        case {validate(qos, Qos), validate(topic, Topic)} of
            {true, true} ->
                emqttd_pubsub:publish(http, #mqtt_message{qos     = Qos,
                                                          retain  = Retain,
                                                          topic   = Topic,
                                                          payload = Message}),
                Req:ok({"text/plan", <<"ok\n">>});
           {false, _} ->
                Req:respond({400, [], <<"Bad QoS">>});
            {_, false} ->
                Req:respond({400, [], <<"Bad Topic">>})
        end;
	false ->
		Req:respond({401, [], <<"Fobbiden">>})
	end;

%%------------------------------------------------------------------------------
%% MQTT Over WebSocket
%%------------------------------------------------------------------------------
handle_request('GET', "/mqtt", Req) ->
    lager:info("Websocket Connection from: ~s", [Req:get(peer)]),
    Upgrade = Req:get_header_value("Upgrade"),
    Proto = Req:get_header_value("Sec-WebSocket-Protocol"),
    case {is_websocket(Upgrade), Proto} of
        {true, "mqtt" ++ _Vsn} ->
            emqttd_ws_client:start_link(Req);
        {false, _} ->
            lager:error("Not WebSocket: Upgrade = ~s", [Upgrade]),
            Req:respond({400, [], <<"Bad Request">>});
        {_, Proto} ->
            lager:error("WebSocket with error Protocol: ~s", [Proto]),
            Req:respond({400, [], <<"Bad WebSocket Protocol">>})
    end;

%%------------------------------------------------------------------------------
%% Get static files
%%------------------------------------------------------------------------------
handle_request('GET', "/" ++ File, Req) ->
    lager:info("HTTP GET File: ~s", [File]),
    mochiweb_request:serve_file(File, docroot(), Req);

handle_request(Method, Path, Req) ->
    lager:error("Unexpected HTTP Request: ~s ~s", [Method, Path]),
	Req:not_found().

%%------------------------------------------------------------------------------
%% basic authorization
%%------------------------------------------------------------------------------
authorized(Req) ->
    case Req:get_header_value("Authorization") of
	undefined ->
		false;
	"Basic " ++ BasicAuth ->
        {Username, Password} = user_passwd(BasicAuth),
        case emqttd_access_control:auth(#mqtt_client{username = Username}, Password) of
            ok ->
                true;
            {error, Reason} ->
                lager:error("HTTP Auth failure: username=~s, reason=~p", [Username, Reason]),
                false
        end
	end.

user_passwd(BasicAuth) ->
	list_to_tuple(binary:split(base64:decode(BasicAuth), <<":">>)). 

validate(qos, Qos) ->
    (Qos >= ?QOS_0) and (Qos =< ?QOS_2); 

validate(topic, Topic) ->
    emqtt_topic:validate({name, Topic}).

int(S) -> list_to_integer(S).

bool("0") -> false;
bool("1") -> true.

is_websocket(Upgrade) -> 
    Upgrade =/= undefined andalso string:to_lower(Upgrade) =:= "websocket".

docroot() ->
    {file, Here} = code:is_loaded(?MODULE),
    Dir = filename:dirname(filename:dirname(Here)),
    filename:join([Dir, "priv", "www"]).

