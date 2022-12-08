%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_release).

-export([
    edition/0,
    edition_longstr/0,
    description/0,
    version/0
]).

-include("emqx_release.hrl").

-define(EMQX_DESCS, #{
    ee => "EMQX Enterprise",
    ce => "EMQX"
}).

-define(EMQX_REL_VSNS, #{
    ee => ?EMQX_RELEASE_EE,
    ce => ?EMQX_RELEASE_CE
}).

%% @doc Return EMQX description.
description() ->
    maps:get(edition(), ?EMQX_DESCS).

%% @doc Return EMQX edition info.
%% Read info from persistent_term at runtime.
%% Or meck this function to run tests for another edition.
-spec edition() -> ce | ee.
-ifdef(EMQX_RELEASE_EDITION).
edition() -> ?EMQX_RELEASE_EDITION.

edition_longstr() -> <<"Enterprise">>.
-else.
edition() -> ce.

edition_longstr() -> <<"Opensource">>.
-endif.

%% @doc Return the release version.
version() ->
    case lists:keyfind(emqx_vsn, 1, ?MODULE:module_info(compile)) of
        %% For TEST build or dependency build.
        false ->
            build_vsn();
        %% For emqx release build
        {_, Vsn} ->
            VsnStr = build_vsn(),
            case string:str(Vsn, VsnStr) of
                1 ->
                    ok;
                _ ->
                    erlang:error(#{
                        reason => version_mismatch,
                        source => VsnStr,
                        built_for => Vsn
                    })
            end,
            Vsn
    end.

build_vsn() ->
    maps:get(edition(), ?EMQX_REL_VSNS).
