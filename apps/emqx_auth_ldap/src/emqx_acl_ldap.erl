%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_acl_ldap).

-include("emqx_auth_ldap.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("eldap2/include/eldap.hrl").
-include_lib("emqx/include/logger.hrl").

-export([ check_acl/5
        , description/0
        ]).

check_acl(ClientInfo, PubSub, Topic, NoMatchAction, State) ->
    case do_check_acl(ClientInfo, PubSub, Topic, NoMatchAction, State) of
        ok -> ?LOG_SENSITIVE(debug,
                    "[LDAP] ACL ignored, Topic: ~p, Action: ~p for Client: ~p",
                    [Topic, PubSub, ClientInfo]);
        {stop, allow} ->
            ?LOG_SENSITIVE(debug,
                           "[LDAP] Allow Topic: ~p, Action: ~p for Client: ~p",
                           [Topic, PubSub, ClientInfo]),
            {stop, allow};
        {stop, deny} ->
            ?LOG_SENSITIVE(debug,
                           "[LDAP] Deny Topic: ~p, Action: ~p for Client: ~p",
                           [Topic, PubSub, ClientInfo]),
            {stop, deny}
    end.

do_check_acl(#{username := <<$$, _/binary>>}, _PubSub, _Topic, _NoMatchAction, _State) ->
    ok;

do_check_acl(#{username := Username}, PubSub, Topic, _NoMatchAction,
             #{device_dn         := DeviceDn,
               match_objectclass := ObjectClass,
               username_attr     := UidAttr,
               custom_base_dn    := CustomBaseDN,
               pool := Pool} = Config) ->

    Filters = maps:get(filters, Config, []),

    ReplaceRules = [{"${username_attr}", UidAttr},
                    {"${user}", binary_to_list(Username)},
                    {"${device_dn}", DeviceDn}],

    Filter = emqx_auth_ldap:prepare_filter(Filters, UidAttr, ObjectClass, ReplaceRules),

    Attribute = case PubSub of
                    publish   -> "mqttPublishTopic";
                    subscribe -> "mqttSubscriptionTopic"
                end,
    Attribute1 = "mqttPubSubTopic",
    ?LOG(debug, "[LDAP] search dn:~p filter:~p, attribute:~p",
         [DeviceDn, Filter, Attribute]),

    BaseDN = emqx_auth_ldap:replace_vars(CustomBaseDN, ReplaceRules),

    case emqx_auth_ldap_cli:search(Pool, BaseDN, Filter, [Attribute, Attribute1]) of
        {error, noSuchObject} ->
            ok;
        {ok, #eldap_search_result{entries = []}} ->
            ok;
        {ok, #eldap_search_result{entries = [Entry]}} ->
            Topics = proplists:get_value(Attribute, Entry#eldap_entry.attributes, [])
                ++ proplists:get_value(Attribute1, Entry#eldap_entry.attributes, []),
            match(Topic, Topics);
        Error ->
            ?LOG(error, "[LDAP] search error:~p", [Error]),
            {stop, deny}
    end.

match(_Topic, []) ->
    ok;

match(Topic, [Filter | Topics]) ->
    case emqx_topic:match(Topic, list_to_binary(Filter)) of
        true  -> {stop, allow};
        false -> match(Topic, Topics)
    end.

description() ->
    "ACL with LDAP".
