%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include("logger.hrl").

-export([ mount/2
        , unmount/2
        ]).

-export([replvar/2]).

-export_type([mountpoint/0]).

-type(mountpoint() :: binary()).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

mount(undefined, Any) ->
    Any;
mount(MountPoint, Topic) when is_binary(Topic) ->
    <<MountPoint/binary, Topic/binary>>;
mount(MountPoint, Msg = #message{topic = Topic}) ->
    Msg#message{topic = <<MountPoint/binary, Topic/binary>>};
mount(MountPoint, TopicFilters) when is_list(TopicFilters) ->
    [{<<MountPoint/binary, Topic/binary>>, SubOpts}
     || {Topic, SubOpts} <- TopicFilters].

unmount(undefined, Msg) ->
    Msg;
%% TODO: Fixme later
unmount(MountPoint, Topic) when is_binary(Topic) ->
    try split_binary(Topic, byte_size(MountPoint)) of
        {MountPoint, Topic1} -> Topic1
    catch
        error:badarg-> Topic
    end;
unmount(MountPoint, Msg = #message{topic = Topic}) ->
    try split_binary(Topic, byte_size(MountPoint)) of
        {MountPoint, Topic1} -> Msg#message{topic = Topic1}
    catch
        error:badarg->
            Msg
    end.

replvar(undefined, _Vars) ->
    undefined;
replvar(MountPoint, #{client_id := ClientId, username := Username}) ->
    lists:foldl(fun feed_var/2, MountPoint, [{<<"%c">>, ClientId}, {<<"%u">>, Username}]).

feed_var({<<"%c">>, ClientId}, MountPoint) ->
    emqx_topic:feed_var(<<"%c">>, ClientId, MountPoint);
feed_var({<<"%u">>, undefined}, MountPoint) ->
    MountPoint;
feed_var({<<"%u">>, Username}, MountPoint) ->
    emqx_topic:feed_var(<<"%u">>, Username, MountPoint).

