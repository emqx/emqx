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

-module(emqttd_backend).

-include("emqttd.hrl").

-include_lib("stdlib/include/ms_transform.hrl").

%% Mnesia Callbacks
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% Retained Message API
-export([retain_message/1, read_messages/1, match_messages/1, delete_message/1,
         expire_messages/1, retained_count/0]).

-record(retained_message, {topic, msg}).

%%--------------------------------------------------------------------
%% Mnesia callbacks
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = emqttd_mnesia:create_table(retained_message, [
                {type, ordered_set},
                {disc_copies, [node()]},
                {record_name, retained_message},
                {attributes, record_info(fields, retained_message)},
                {storage_properties, [{ets, [compressed]},
                                      {dets, [{auto_save, 1000}]}]}]);

mnesia(copy) ->
    ok = emqttd_mnesia:copy_table(retained_message).

%%--------------------------------------------------------------------
%% Retained Message
%%--------------------------------------------------------------------

-spec(retain_message(mqtt_message()) -> ok).
retain_message(Msg = #mqtt_message{topic = Topic}) ->
    mnesia:dirty_write(#retained_message{topic = Topic, msg = Msg}).

-spec(read_messages(binary()) -> [mqtt_message()]).
read_messages(Topic) ->
    [Msg || #retained_message{msg = Msg} <- mnesia:dirty_read(retained_message, Topic)].

-spec(match_messages(binary()) -> [mqtt_message()]).
match_messages(Filter) ->
    %% TODO: optimize later...
    Fun = fun(#retained_message{topic = Name, msg = Msg}, Acc) ->
            case emqttd_topic:match(Name, Filter) of
                true -> [Msg|Acc];
                false -> Acc
            end
          end,
    mnesia:async_dirty(fun mnesia:foldl/3, [Fun, [], retained_message]).

-spec(delete_message(binary()) -> ok).
delete_message(Topic) ->
    mnesia:dirty_delete(retained_message, Topic).

-spec(expire_messages(pos_integer()) -> any()).
expire_messages(Time) when is_integer(Time) ->
    mnesia:transaction(
        fun() ->
            Match = ets:fun2ms(
                        fun(#retained_message{topic = Topic, msg = #mqtt_message{timestamp = {MegaSecs, Secs, _}}})
                            when Time > (MegaSecs * 1000000 + Secs) -> Topic
                        end),
            Topics = mnesia:select(retained_message, Match, write),
            lists:foreach(fun(<<"$SYS/", _/binary>>) -> ok; %% ignore $SYS/# messages
                             (Topic) -> mnesia:delete({retained_message, Topic})
                           end, Topics)
        end).

-spec(retained_count() -> non_neg_integer()).
retained_count() ->
    mnesia:table_info(retained_message, size).

