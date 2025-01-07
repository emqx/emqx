%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_release_tests).

-include_lib("eunit/include/eunit.hrl").

vsn_compre_test_() ->
    CurrentVersion = emqx_release:version_with_prefix(),
    [
        {"must be 'same' when comparing with current version", fun() ->
            ?assertEqual(same, emqx_release:vsn_compare(CurrentVersion))
        end},
        {"must be 'same' when comparing same version strings", fun() ->
            ?assertEqual(same, emqx_release:vsn_compare("1.1.1", "1.1.1"))
        end},
        {"1.1.1 is older than 1.1.2", fun() ->
            ?assertEqual(older, emqx_release:vsn_compare("1.1.2", "1.1.1")),
            ?assertEqual(newer, emqx_release:vsn_compare("1.1.1", "1.1.2"))
        end},
        {"1.1.9 is older than 1.1.10", fun() ->
            ?assertEqual(older, emqx_release:vsn_compare("1.1.10", "1.1.9")),
            ?assertEqual(newer, emqx_release:vsn_compare("1.1.9", "1.1.10"))
        end},
        {"alpha is older than beta", fun() ->
            ?assertEqual(older, emqx_release:vsn_compare("1.1.1-beta.1", "1.1.1-alpha.2")),
            ?assertEqual(newer, emqx_release:vsn_compare("1.1.1-alpha.2", "1.1.1-beta.1"))
        end},
        {"beta is older than rc", fun() ->
            ?assertEqual(older, emqx_release:vsn_compare("1.1.1-rc.1", "1.1.1-beta.2")),
            ?assertEqual(newer, emqx_release:vsn_compare("1.1.1-beta.2", "1.1.1-rc.1"))
        end},
        {"rc is older than official cut", fun() ->
            ?assertEqual(older, emqx_release:vsn_compare("1.1.1", "1.1.1-rc.1")),
            ?assertEqual(newer, emqx_release:vsn_compare("1.1.1-rc.1", "1.1.1"))
        end},
        {"git hash suffix is ignored", fun() ->
            ?assertEqual(older, emqx_release:vsn_compare("1.1.1-gabcd", "1.1.1-rc.1-g1234")),
            ?assertEqual(newer, emqx_release:vsn_compare("1.1.1-rc.1-gabcd", "1.1.1-g1234"))
        end},
        {"invalid version string will crash", fun() ->
            ?assertError({invalid_version_string, "1.1.a"}, emqx_release:vsn_compare("v1.1.a")),
            ?assertError(
                {invalid_version_string, "1.1.1-alpha"}, emqx_release:vsn_compare("e1.1.1-alpha")
            )
        end}
    ].

emqx_flavor_test() ->
    case emqx_release:edition() of
        ce ->
            ok;
        ee ->
            ?assertEqual(official, emqx_release:get_flavor()),
            ?assertEqual("EMQX Enterprise", emqx_app:get_description()),
            emqx_release:set_flavor(marketplace),
            ?assertEqual(marketplace, emqx_release:get_flavor()),
            ?assertEqual("EMQX Enterprise(marketplace)", emqx_app:get_description()),
            emqx_release:set_flavor(official)
    end.
