%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    edition_vsn_prefix/0,
    edition_longstr/0,
    description/0,
    version/0,
    version_with_prefix/0,
    vsn_compare/1,
    vsn_compare/2,
    get_flavor/0,
    on_load/0
]).

-ifdef(TEST).
-export([set_flavor/1]).
-endif.

-on_load(on_load/0).

-include("emqx_release.hrl").

-ifndef(EMQX_RELEASE_EDITION).
-define(EMQX_RELEASE_EDITION, ce).
-endif.

-define(EMQX_DESCS, #{
    ee =>
        case get_flavor() of
            official -> "EMQX Enterprise";
            Flavor -> io_lib:format("EMQX Enterprise(~s)", [Flavor])
        end,
    ce => "EMQX"
}).

-define(EMQX_REL_NAME, #{
    ee => <<"Enterprise">>,
    ce => <<"Opensource">>
}).

-define(EMQX_REL_VSNS, #{
    ee => ?EMQX_RELEASE_EE,
    ce => ?EMQX_RELEASE_CE
}).

-define(EMQX_REL_VSN_PREFIX, #{
    ee => "e",
    ce => "v"
}).

%% @hidden Initialize edition. Almost static. use persistent_term to trick compiler.
-spec on_load() -> ok.
on_load() ->
    persistent_term:put('EMQX_RELEASE_EDITION', ?EMQX_RELEASE_EDITION).

%% @doc Return EMQX description.
-dialyzer({[no_match], [description/0]}).
description() ->
    maps:get(edition(), ?EMQX_DESCS).

%% @doc Return EMQX edition info.
%% Read info from persistent_term at runtime.
%% Or meck this function to run tests for another edition.
-spec edition() -> ce | ee.
edition() ->
    persistent_term:get('EMQX_RELEASE_EDITION').

%% @doc Return EMQX version prefix string.
edition_vsn_prefix() ->
    maps:get(edition(), ?EMQX_REL_VSN_PREFIX).

%% @doc Return EMQX edition name, ee => Enterprise ce => Opensource.
edition_longstr() ->
    maps:get(edition(), ?EMQX_REL_NAME).

%% @doc Return the release version with prefix.
version_with_prefix() ->
    edition_vsn_prefix() ++ version().

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

%% @doc Compare the given version with the current running version,
%% return 'newer' 'older' or 'same'.
vsn_compare("v" ++ Vsn) ->
    vsn_compare(?EMQX_RELEASE_CE, Vsn);
vsn_compare("e" ++ Vsn) ->
    vsn_compare(?EMQX_RELEASE_EE, Vsn).

%% @private Compare the second argument with the first argument, return
%% 'newer' 'older' or 'same' semver comparison result.
vsn_compare(Vsn1, Vsn2) ->
    ParsedVsn1 = parse_vsn(Vsn1),
    ParsedVsn2 = parse_vsn(Vsn2),
    case ParsedVsn1 =:= ParsedVsn2 of
        true ->
            same;
        false when ParsedVsn1 < ParsedVsn2 ->
            newer;
        false ->
            older
    end.

%% @private Parse the version string to a tuple.
%% Return {{Major, Minor, Patch}, Suffix}.
%% Where Suffix is either an empty string or a tuple like {"rc", 1}.
%% NOTE: taking the nature ordering of the suffix:
%% {"alpha", _} < {"beta", _} < {"rc", _} < ""
parse_vsn(Vsn) ->
    try
        [V1, V2, V3 | Suffix0] = string:tokens(Vsn, ".-"),
        Suffix =
            case Suffix0 of
                "" ->
                    %% "5.1.0"
                    "";
                ["g" ++ _] ->
                    %% "5.1.0-g53ab85b1"
                    "";
                [ReleaseStage, Number | _] ->
                    %% "5.1.0-rc.1" or "5.1.0-rc.1-g53ab85b1"
                    {ReleaseStage, list_to_integer(Number)}
            end,
        {{list_to_integer(V1), list_to_integer(V2), list_to_integer(V3)}, Suffix}
    catch
        _:_ ->
            erlang:error({invalid_version_string, Vsn})
    end.

-spec get_flavor() -> atom().
-ifdef(TEST).
set_flavor(Flavor) when is_atom(Flavor) ->
    persistent_term:put({?MODULE, 'EMQX_FLAVOR'}, Flavor).

get_flavor() ->
    persistent_term:get({?MODULE, 'EMQX_FLAVOR'}, official).
-else.

-ifndef(EMQX_FLAVOR).
get_flavor() ->
    official.
-else.
get_flavor() ->
    ?EMQX_FLAVOR.
-endif.

-endif.
