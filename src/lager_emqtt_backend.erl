%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>. All Rights Reserved.
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
%%% @doc emqtt lager backend
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%-----------------------------------------------------------------------------
-module(lager_emqtt_backend).

-behaviour(gen_event).

-include_lib("lager/include/lager.hrl").

-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {level :: {'mask', integer()},
                formatter :: atom(),
                format_config :: any()}).

-define(DEFAULT_FORMAT, [time, " ", pid, " [",severity, "] ", message]).

init([Level]) when is_atom(Level) ->
    init(Level);

init(Level) when is_atom(Level) ->
    init([Level,{lager_default_formatter, ?DEFAULT_FORMAT}]);

init([Level,{Formatter, FormatterConfig}]) when is_atom(Formatter) ->
    Levels = lager_util:config_to_mask(Level),
    {ok, #state{level = Levels, formatter = Formatter,
                format_config = FormatterConfig}}.

handle_call(get_loglevel, #state{level = Level} = State) ->
    {ok, Level, State};

handle_call({set_loglevel, Level}, State) ->
    try lager_util:config_to_mask(Level) of
        Levels -> {ok, ok, State#state{level = Levels}}
    catch
        _:_ -> {ok, {error, bad_log_level}, State}
    end;

handle_call(_Request, State) ->
    {ok, ok, State}.

handle_event({log, Message}, State = #state{level = L}) ->
    case lager_util:is_loggable(Message, L, ?MODULE) of
        true ->
            publish_log(Message, State);
        false ->
            {ok, State}
    end;

handle_event(_Event, State) ->
    {ok, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

publish_log(Message, State = #state{formatter = Formatter,
                                    format_config = FormatConfig}) ->
    Severity = lager_msg:severity(Message),
    Payload = Formatter:format(Message, FormatConfig),
    emqttd_pubsub:publish(
      emqttd_message:make(
        log, topic(Severity), iolist_to_binary(Payload))),
    {ok, State}.

topic(Severity) ->
    emqttd_topic:systop(list_to_binary(lists:concat(['logs/', Severity]))).

