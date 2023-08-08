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

-module(emqx_ldap_authz).

-include_lib("emqx_authz/include/emqx_authz.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").
-include_lib("eldap/include/eldap.hrl").

-behaviour(emqx_authz).

-define(PREPARE_KEY, ?MODULE).

%% AuthZ Callbacks
-export([
    description/0,
    create/1,
    update/1,
    destroy/1,
    authorize/4
]).

-export([fields/1]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

fields(config) ->
    emqx_authz_schema:authz_common_fields(ldap) ++
        [
            {publish_attribute, attribute_meta(publish_attribute, <<"mqttPublishTopic">>)},
            {subscribe_attribute, attribute_meta(subscribe_attribute, <<"mqttSubscriptionTopic">>)},
            {all_attribute, attribute_meta(all_attribute, <<"mqttPubSubTopic">>)},
            {query_timeout,
                ?HOCON(
                    emqx_schema:timeout_duration_ms(),
                    #{
                        desc => ?DESC(query_timeout),
                        default => <<"5s">>
                    }
                )}
        ] ++
        emqx_ldap:fields(config).

attribute_meta(Name, Default) ->
    ?HOCON(
        string(),
        #{
            default => Default,
            desc => ?DESC(Name)
        }
    ).

%%------------------------------------------------------------------------------
%% AuthZ Callbacks
%%------------------------------------------------------------------------------

description() ->
    "AuthZ with LDAP".

create(Source) ->
    ResourceId = emqx_authz_utils:make_resource_id(?MODULE),
    {ok, _Data} = emqx_authz_utils:create_resource(ResourceId, emqx_ldap, Source),
    Annotations = new_annotations(#{id => ResourceId}, Source),
    Source#{annotations => Annotations}.

update(Source) ->
    case emqx_authz_utils:update_resource(emqx_ldap, Source) of
        {error, Reason} ->
            error({load_config_error, Reason});
        {ok, Id} ->
            Annotations = new_annotations(#{id => Id}, Source),
            Source#{annotations => Annotations}
    end.

destroy(#{annotations := #{id := Id}}) ->
    ok = emqx_resource:remove_local(Id).

authorize(
    Client,
    Action,
    Topic,
    #{
        query_timeout := QueryTimeout,
        annotations := #{id := ResourceID} = Annotations
    }
) ->
    Attrs = select_attrs(Action, Annotations),
    case emqx_resource:simple_sync_query(ResourceID, {query, Client, Attrs, QueryTimeout}) of
        {ok, []} ->
            nomatch;
        {ok, [Entry | _]} ->
            do_authorize(Action, Topic, Attrs, Entry);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "query_ldap_error",
                reason => Reason,
                resource_id => ResourceID
            }),
            nomatch
    end.

do_authorize(Action, Topic, [Attr | T], Entry) ->
    Topics = proplists:get_value(Attr, Entry#eldap_entry.attributes, []),
    case match_topic(Topic, Topics) of
        true ->
            {matched, allow};
        false ->
            do_authorize(Action, Topic, T, Entry)
    end;
do_authorize(_Action, _Topic, [], _Entry) ->
    nomatch.

new_annotations(Init, Source) ->
    lists:foldl(
        fun(Attr, Acc) ->
            Acc#{
                Attr =>
                    case maps:get(Attr, Source) of
                        Value when is_binary(Value) ->
                            erlang:binary_to_list(Value);
                        Value ->
                            Value
                    end
            }
        end,
        Init,
        [publish_attribute, subscribe_attribute, all_attribute]
    ).

select_attrs(#{action_type := publish}, #{publish_attribute := Pub, all_attribute := All}) ->
    [Pub, All];
select_attrs(_, #{subscribe_attribute := Sub, all_attribute := All}) ->
    [Sub, All].

match_topic(Target, Topics) ->
    lists:any(
        fun(Topic) ->
            emqx_topic:match(Target, erlang:list_to_binary(Topic))
        end,
        Topics
    ).
