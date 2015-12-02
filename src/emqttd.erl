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
%%% emqttd main module.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd).

-author("Feng Lee <feng@emqtt.io>").

-export([start/0, env/1, env/2,
         open_listeners/1, close_listeners/1,
         load_all_mods/0, is_mod_enabled/1,
         is_running/1]).

-define(MQTT_SOCKOPTS, [
        binary,
        {packet,    raw},
        {reuseaddr, true},
        {backlog,   512},
        {nodelay,   true}
]).

-type listener() :: {atom(), inet:port_number(), [esockd:option()]}.

%%------------------------------------------------------------------------------
%% @doc Start emqttd application.
%% @end
%%------------------------------------------------------------------------------
-spec start() -> ok | {error, any()}.
start() ->
    application:start(emqttd).

%%------------------------------------------------------------------------------
%% @doc Get environment
%% @end
%%------------------------------------------------------------------------------
-spec env(atom()) -> list().
env(Group) ->
    application:get_env(emqttd, Group, []).

-spec env(atom(), atom()) -> undefined | any().
env(Group, Name) ->
    proplists:get_value(Name, env(Group)).

%%------------------------------------------------------------------------------
%% @doc Open Listeners
%% @end
%%------------------------------------------------------------------------------
-spec open_listeners([listener()]) -> any().
open_listeners(Listeners) when is_list(Listeners) ->
    [open_listener(Listener) || Listener <- Listeners].

%% open mqtt port
open_listener({mqtt, Port, Options}) ->
    open_listener(mqtt, Port, Options);

%% open mqtt(SSL) port
open_listener({mqtts, Port, Options}) ->
    open_listener(mqtts, Port, Options);

%% open http port
open_listener({http, Port, Options}) ->
    MFArgs = {emqttd_http, handle_request, []},
    mochiweb:start_http(Port, Options, MFArgs);

%% open https port
open_listener({https, Port, Options}) ->
    MFArgs = {emqttd_http, handle_request, []},
    mochiweb:start_http(Port, Options, MFArgs).

open_listener(Protocol, Port, Options) ->
    MFArgs = {emqttd_client, start_link, [env(mqtt)]},
    esockd:open(Protocol, Port, merge_sockopts(Options) , MFArgs).

merge_sockopts(Options) ->
    SockOpts = emqttd_opts:merge(?MQTT_SOCKOPTS,
                                 proplists:get_value(sockopts, Options, [])),
    Updated = [fix_address(Opt) || Opt <- SockOpts],
    emqttd_opts:merge(Options, [{sockopts, Updated}]).

%%------------------------------------------------------------------------------
%% @doc Close Listeners
%% @end
%%------------------------------------------------------------------------------
-spec close_listeners([listener()]) -> any().
close_listeners(Listeners) when is_list(Listeners) ->
    [close_listener(Listener) || Listener <- Listeners].

close_listener({Protocol, Port, _Options}) ->
    esockd:close({Protocol, Port}).

load_all_mods() ->
    lists:foreach(fun({Name, Opts}) ->
        Mod = list_to_atom("emqttd_mod_" ++ atom_to_list(Name)),
        Mod:load(Opts),
        lager:info("load module ~s successfully", [Name])
    end, env(modules)).

is_mod_enabled(Name) ->
    env(modules, Name) =/= undefined.

%%------------------------------------------------------------------------------
%% @doc Is running?
%% @end
%%------------------------------------------------------------------------------
is_running(Node) ->
    case rpc:call(Node, erlang, whereis, [emqttd]) of
        {badrpc, _}          -> false;
        undefined            -> false;
        Pid when is_pid(Pid) -> true
    end.


%%------------------------------------------------------------------------------
%% @doc Parse string address to tuple version
%% @end
%%------------------------------------------------------------------------------
fix_address({ip, Address}) when is_list(Address) ->
    {ok, Addr} = inet:parse_address(Address),
    {ip, Addr};
fix_address({ip, Address}) when is_binary(Address) ->
    fix_address({ip, binary_to_list(Address)});
fix_address(Opt) ->
    Opt.
