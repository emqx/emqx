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
    Msg = emqttd_message:make(log, topic(Severity), iolist_to_binary(Payload)),
    emqttd:publish(emqttd_message:set_flag(sys, Msg)),
    {ok, State}.

topic(Severity) ->
    emqttd_topic:systop(list_to_binary(lists:concat(['logs/', Severity]))).

