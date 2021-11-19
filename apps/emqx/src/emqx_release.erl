%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([ edition/0
        , put_edition/0
        , put_edition/1
        , description/0
        , version/0
        ]).

-include("emqx_release.hrl").

%% @doc Return EMQ X description.
description() ->
    case os:getenv("EMQX_DESCRIPTION") of
        false -> "EMQ X Community Edition";
        "" -> "EMQ X Community Edition";
        Str -> string:strip(Str, both, $\n)
    end.

%% @doc Return EMQ X edition info.
%% Read info from persistent_term at runtime.
%% Or meck this function to run tests for another eidtion.
-spec edition() -> ce | ee | edge.
edition() ->
    try persistent_term:get(emqx_edition)
    catch error : badarg -> get_edition() end.

%% @private initiate EMQ X edition info in persistent_term.
put_edition() ->
    ok = put_edition(get_edition()).

%% @hidden This function is mostly for testing.
%% Switch to another eidtion at runtime to run edition-specific tests.
-spec put_edition(ce | ee | edge) -> ok.
put_edition(Which) ->
    persistent_term:put(emqx_edition, Which),
    ok.

-spec get_edition() -> ce | ee | edge.
get_edition() ->
    edition(description()).

edition(Desc) ->
    case re:run(Desc, "enterprise", [caseless]) of
        {match, _} ->
            ee;
        _ ->
            case re:run(Desc, "edge", [caseless]) of
                {match, _} -> edge;
                _ -> ce
            end
    end.

%% @doc Return the release version.
version() ->
    case lists:keyfind(emqx_vsn, 1, ?MODULE:module_info(compile)) of
        false ->    %% For TEST build or depedency build.
            ?EMQX_RELEASE;
        {_, Vsn} -> %% For emqx release build
            VsnStr = ?EMQX_RELEASE,
            case string:str(Vsn, VsnStr) of
                1 -> ok;
                _ ->
                    erlang:error(#{ reason => version_mismatch
                                  , source => VsnStr
                                  , built_for => Vsn
                                  })
            end,
            Vsn
    end.
