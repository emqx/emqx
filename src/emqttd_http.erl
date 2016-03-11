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

%% @doc emqttd http publish API and websocket client.
-module(emqttd_http).

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-import(proplists, [get_value/2, get_value/3]).

-export([handle_request/1]).

handle_request(Req) ->
    handle_request(Req:get(method), Req:get(path), Req).

handle_request('GET', "/status", Req) ->
    {InternalStatus, _ProvidedStatus} = init:get_status(),
    AppStatus =
    case lists:keysearch(emqttd, 1, application:which_applications()) of
        false         -> not_running;
        {value, _Val} -> running
    end,
    Status = io_lib:format("Node ~s is ~s~nemqttd is ~s",
                            [node(), InternalStatus, AppStatus]),
    Req:ok({"text/plain", iolist_to_binary(Status)});

%%--------------------------------------------------------------------
%% HTTP Publish API
%%--------------------------------------------------------------------

handle_request('POST', "/mqtt/publish", Req) ->
    case authorized(Req) of
        true  -> http_publish(Req);
        false -> Req:respond({401, [], <<"Fobbiden">>})
    end;

%%--------------------------------------------------------------------
%% MQTT Over WebSocket
%%--------------------------------------------------------------------
handle_request('GET', "/mqtt", Req) ->
    lager:info("WebSocket Connection from: ~s", [Req:get(peer)]),
    Upgrade = Req:get_header_value("Upgrade"),
    Proto   = Req:get_header_value("Sec-WebSocket-Protocol"),
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

%%--------------------------------------------------------------------
%% Get static files
%%--------------------------------------------------------------------

handle_request('GET', "/" ++ File, Req) ->
    lager:info("HTTP GET File: ~s", [File]),
    mochiweb_request:serve_file(File, docroot(), Req);

handle_request(Method, Path, Req) ->
    lager:error("Unexpected HTTP Request: ~s ~s", [Method, Path]),
    Req:not_found().

%%--------------------------------------------------------------------
%% HTTP Publish
%%--------------------------------------------------------------------

http_publish(Req) ->
    Params = mochiweb_request:parse_post(Req),
    lager:info("HTTP Publish: ~p", [Params]),
    Topics   = topics(Params),
    ClientId = get_value("client", Params, http),
    Qos      = int(get_value("qos", Params, "0")),
    Retain   = bool(get_value("retain", Params, "0")),
    Payload  = list_to_binary(get_value("message", Params)),
    case {validate(qos, Qos), validate(topics, Topics)} of
        {true, true} ->
            lists:foreach(fun(Topic) ->
                Msg = emqttd_message:make(ClientId, Qos, Topic, Payload),
                emqttd:publish(Msg#mqtt_message{retain  = Retain})
            end, Topics),
            Req:ok({"text/plain", <<"ok">>});
       {false, _} ->
            Req:respond({400, [], <<"Bad QoS">>});
        {_, false} ->
            Req:respond({400, [], <<"Bad Topics">>})
    end.

topics(Params) ->
    Tokens = [get_value("topic", Params) | string:tokens(get_value("topics", Params, ""), ",")],
    [list_to_binary(Token) || Token <- Tokens, Token =/= undefined].

validate(qos, Qos) ->
    (Qos >= ?QOS_0) and (Qos =< ?QOS_2);

validate(topics, [Topic|Left]) ->
    case validate(topic, Topic) of
        true  -> validate(topics, Left);
        false -> false
    end;
validate(topics, []) ->
    true;

validate(topic, Topic) ->
    emqttd_topic:validate({name, Topic}).

%%--------------------------------------------------------------------
%% basic authorization
%%--------------------------------------------------------------------

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


int(S) -> list_to_integer(S).

bool("0") -> false;
bool("1") -> true.

is_websocket(Upgrade) -> 
    Upgrade =/= undefined andalso string:to_lower(Upgrade) =:= "websocket".

docroot() ->
    {file, Here} = code:is_loaded(?MODULE),
    Dir = filename:dirname(filename:dirname(Here)),
    filename:join([Dir, "priv", "www"]).

