%%--------------------------------------------------------------------
%% Copyright (c) 2017-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_relup).

%% NOTE: DO NOT remove this `-include`.
%% We use this to force this module to be upgraded every release.
-include("emqx_release.hrl").

-export([
    post_release_upgrade/2,
    post_release_downgrade/2
]).

-define(INFO(FORMAT), io:format("[emqx_relup] " ++ FORMAT ++ "~n")).
-define(INFO(FORMAT, ARGS), io:format("[emqx_relup] " ++ FORMAT ++ "~n", ARGS)).

%% What to do after upgraded from an old release vsn.
post_release_upgrade(FromRelVsn, _) ->
    ?INFO("emqx has been upgraded from ~s to ~s!", [FromRelVsn, emqx_release:version()]),
    reload_components().

%% What to do after downgraded to an old release vsn.
post_release_downgrade(ToRelVsn, _) ->
    ?INFO("emqx has been downgraded from ~s to ~s!", [emqx_release:version(), ToRelVsn]),
    reload_components().

reload_components() ->
    ok.
