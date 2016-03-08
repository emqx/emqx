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

%% Mnesia Callbacks
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% API.
-export([add_static_subscription/1, lookup_static_subscriptions/1,
         del_static_subscriptions/1, del_static_subscription/2]).

%%--------------------------------------------------------------------
%% Mnesia callbacks
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = emqttd_mnesia:create_table(static_subscription, [
                {type, bag},
                {disc_copies, [node()]},
                {record_name, mqtt_subscription},
                {attributes, record_info(fields, mqtt_subscription)},
                {storage_properties, [{ets, [compressed]},
                                      {dets, [{auto_save, 5000}]}]}]);

mnesia(copy) ->
    ok = emqttd_mnesia:copy_table(static_subscription).

%%--------------------------------------------------------------------
%% Static Subscriptions
%%--------------------------------------------------------------------

%% @doc Add a static subscription manually.
-spec add_static_subscription(mqtt_subscription()) -> {atom, ok}.
add_static_subscription(Subscription = #mqtt_subscription{subid = SubId, topic = Topic}) ->
    Pattern = match_pattern(SubId, Topic),
    mnesia:transaction(
        fun() ->
            case mnesia:match_object(static_subscription, Pattern, write) of
                [] ->
                    mnesia:write(static_subscription, Subscription, write);
                [Subscription] ->
                    mnesia:abort({error, existed});
                [Subscription1] -> %% QoS is different
                    mnesia:delete_object(static_subscription, Subscription1, write),
                    mnesia:write(static_subscription, Subscription, write)
            end
        end).

%% @doc Lookup static subscriptions.
-spec lookup_static_subscriptions(binary()) -> list(mqtt_subscription()).
lookup_static_subscriptions(ClientId) when is_binary(ClientId) ->
    mnesia:dirty_read(static_subscription, ClientId).

%% @doc Delete static subscriptions by ClientId manually.
-spec del_static_subscriptions(binary()) -> ok.
del_static_subscriptions(ClientId) when is_binary(ClientId) ->
    mnesia:transaction(fun mnesia:delete/1, [{static_subscription, ClientId}]).

%% @doc Delete a static subscription manually.
-spec del_static_subscription(binary(), binary()) -> ok.
del_static_subscription(ClientId, Topic) when is_binary(ClientId) andalso is_binary(Topic) ->
    mnesia:transaction(fun del_static_subscription_/1, [match_pattern(ClientId, Topic)]).

del_static_subscription_(Pattern) ->
    lists:foreach(fun(Subscription) ->
                mnesia:delete_object(static_subscription, Subscription, write)
        end, mnesia:match_object(static_subscription, Pattern, write)).

match_pattern(SubId, Topic) ->
    #mqtt_subscription{subid = SubId, topic = Topic, qos = '_'}.

