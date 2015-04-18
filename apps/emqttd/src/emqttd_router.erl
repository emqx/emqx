%%-----------------------------------------------------------------------------
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

%%TODO: route chain... statistics
-module(emqttd_router).

-include_lib("emqtt/include/emqtt.hrl").

-include("emqttd.hrl").

-behaviour(gen_server).

-define(SERVER, ?MODULE).

%% API Function Exports
-export([start_link/0]).

%%Router Chain--> --->In Out<---
-export([route/2]).

%% Mnesia Callbacks
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {}).

%%%=============================================================================
%%% Mnesia callbacks
%%%=============================================================================
mnesia(boot) ->
    %% topic table
    ok = emqttd_mnesia:create_table(topic, [
                {type, bag},
                {ram_copies, [node()]},
                {record_name, mqtt_topic},
                {attributes, record_info(fields, mqtt_topic)}]).

mnesia(copy) ->
    ok = emqttd_mnesia:copy_table(topic),

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Start emqttd router.
%%
%% @end
%%------------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% @doc
%% Route mqtt message. From is clienid or module.
%%
%% @end
%%------------------------------------------------------------------------------
-spec route(From :: binary() | atom(), Msg :: mqtt_message()) -> ok.
route(From, Msg) ->
    lager:info("Route ~s from ~s", [emqtt_message:format(Msg), From]),
    emqttd_msg_store:retain(Msg),
	emqttd_pubsub:publish(emqtt_message:unset_flag(Msg)).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================
init([]) ->
    TabId = ets:new(?CLIENT_TABLE, [bag,
                                    named_table,
                                    public,
                                    {read_concurrency, true}]),
    %% local subscriber table, not shared with other nodes 
    ok = emqttd_mnesia:create_table(subscriber, [
                {type, bag},
                {ram_copies, [node()]},
                {record_name, mqtt_subscriber},
                {attributes, record_info(fields, mqtt_subscriber)},
                {index, [subpid]},
                {local_content, true}]);

    {ok, #state{tab = TabId}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
	
%%%=============================================================================
%%% Internal functions
%%%=============================================================================

