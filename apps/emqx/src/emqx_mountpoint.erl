%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mountpoint).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("emqx_placeholder.hrl").
-include("types.hrl").

-export([
    mount/2,
    unmount/2
]).

-export([replvar/2]).

-export([lookup/2]).

-export_type([mountpoint/0]).

-type mountpoint() :: binary().

-define(ALLOWED_VARS, [
    ?VAR_CLIENTID,
    ?VAR_USERNAME,
    ?VAR_ENDPOINT_NAME,
    ?VAR_ZONE,
    ?VAR_NS_CLIENT_ATTRS
]).

-spec mount(option(mountpoint()), Any) -> Any when
    Any ::
        emqx_types:topic()
        | emqx_types:share()
        | emqx_types:message()
        | emqx_types:topic_filters().
mount(undefined, Any) ->
    Any;
mount(MountPoint, Topic) when ?IS_TOPIC(Topic) ->
    prefix_maybe_share(MountPoint, Topic);
mount(MountPoint, Msg = #message{topic = Topic}) when is_binary(Topic) ->
    Msg#message{topic = prefix_maybe_share(MountPoint, Topic)};
mount(MountPoint, TopicFilters) when is_list(TopicFilters) ->
    [{prefix_maybe_share(MountPoint, Topic), SubOpts} || {Topic, SubOpts} <- TopicFilters].

-spec prefix_maybe_share(option(mountpoint()), Any) -> Any when
    Any ::
        emqx_types:topic()
        | emqx_types:share().
prefix_maybe_share(MountPoint, Topic) when
    is_binary(MountPoint) andalso is_binary(Topic)
->
    <<MountPoint/binary, Topic/binary>>;
prefix_maybe_share(MountPoint, #share{group = Group, topic = Topic}) when
    is_binary(MountPoint) andalso is_binary(Topic)
->
    #share{group = Group, topic = prefix_maybe_share(MountPoint, Topic)}.

-spec unmount(option(mountpoint()), Any) -> Any when
    Any ::
        emqx_types:topic()
        | emqx_types:share()
        | emqx_types:message().
unmount(undefined, Any) ->
    Any;
unmount(MountPoint, Topic) when ?IS_TOPIC(Topic) ->
    unmount_maybe_share(MountPoint, Topic);
unmount(MountPoint, Msg = #message{topic = Topic}) when is_binary(Topic) ->
    Msg#message{topic = unmount_maybe_share(MountPoint, Topic)}.

unmount_maybe_share(MountPoint, Topic) when
    is_binary(MountPoint) andalso is_binary(Topic)
->
    case string:prefix(Topic, MountPoint) of
        nomatch -> Topic;
        Topic1 -> Topic1
    end;
unmount_maybe_share(MountPoint, TopicFilter = #share{topic = Topic}) when
    is_binary(MountPoint) andalso is_binary(Topic)
->
    TopicFilter#share{topic = unmount_maybe_share(MountPoint, Topic)}.

-spec replvar(option(mountpoint()), map()) -> option(mountpoint()).
replvar(undefined, _Vars) ->
    undefined;
replvar(MountPoint, Vars) ->
    Template = parse(MountPoint),
    {String, _Errors} = emqx_template:render(Template, {?MODULE, Vars}),
    unicode:characters_to_binary(String).

lookup([<<?VAR_CLIENTID>>], #{clientid := ClientId}) when is_binary(ClientId) ->
    {ok, ClientId};
lookup([<<?VAR_USERNAME>>], #{username := Username}) when is_binary(Username) ->
    {ok, Username};
lookup([<<?VAR_ENDPOINT_NAME>>], #{endpoint_name := Name}) when is_binary(Name) ->
    {ok, Name};
lookup([<<?VAR_ZONE>>], #{zone := Zone}) ->
    {ok, atom_to_binary(Zone)};
lookup([<<"client_attrs">>, AttrName], #{client_attrs := Attrs}) when is_map(Attrs) ->
    Original = iolist_to_binary(["${client_attrs.", AttrName, "}"]),
    {ok, maps:get(AttrName, Attrs, Original)};
lookup(Accessor, _) ->
    {ok, iolist_to_binary(["${", lists:join(".", Accessor), "}"])}.

parse(Template) ->
    Parsed = emqx_template:parse(Template),
    case emqx_template:validate(?ALLOWED_VARS, Parsed) of
        ok ->
            Parsed;
        {error, _Disallowed} ->
            Escaped = emqx_template:escape_disallowed(Parsed, ?ALLOWED_VARS),
            emqx_template:parse(Escaped)
    end.
