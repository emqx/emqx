%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_utils_uri_tests).

-include_lib("eunit/include/eunit.hrl").

canonicalize_path_test_() ->
    Cases = [
        {<<"/">>, {ok, <<"/">>}},
        {<<"/api">>, {ok, <<"/api">>}},
        {<<"/api/">>, {ok, <<"/api/">>}},
        {<<"/api/weather">>, {ok, <<"/api/weather">>}},
        %% Dot segments are resolved.
        {<<"/api/v1/../../admin/users">>, {ok, <<"/admin/users">>}},
        {<<"/api/sub/../weather">>, {ok, <<"/api/weather">>}},
        {<<"/api/x/./y">>, {ok, <<"/api/x/y">>}},
        {<<"/api/.">>, {ok, <<"/api">>}},
        %% `..' at the root is dropped.
        {<<"/..">>, {ok, <<"/">>}},
        {<<"/../..">>, {ok, <<"/">>}},
        %% Percent-encoded dot segments are decoded before resolution.
        {<<"/api/v1/%2e%2e/%2e%2e/admin">>, {ok, <<"/admin">>}},
        %% Encoded text round-trips through decode + re-encode.
        {<<"/api/hello%20world">>, {ok, <<"/api/hello%20world">>}},
        {<<"/api/caf%C3%A9">>, {ok, <<"/api/caf%C3%A9">>}},
        %% A segment decoding to a separator is rejected.
        {<<"/api/..%2f..%2fadmin">>, error},
        {<<"/api/a%5Cb">>, error},
        %% Invalid percent encoding and non-UTF-8 bytes are rejected.
        {<<"/api/%zz">>, error},
        {<<"/api/%ff">>, error},
        %% Only absolute paths are accepted.
        {<<"api/relative">>, error},
        {<<>>, error}
    ],
    [
        {Path, ?_assertEqual(Expected, emqx_utils_uri:canonicalize_path(Path))}
     || {Path, Expected} <- Cases
    ].

remove_dot_segments_test_() ->
    [
        ?_assertEqual([], emqx_utils_uri:remove_dot_segments([])),
        ?_assertEqual([<<"a">>, <<"b">>], emqx_utils_uri:remove_dot_segments([<<"a">>, <<"b">>])),
        ?_assertEqual(
            [<<"a">>, <<"c">>],
            emqx_utils_uri:remove_dot_segments([<<"a">>, <<".">>, <<"c">>])
        ),
        ?_assertEqual(
            [<<"c">>],
            emqx_utils_uri:remove_dot_segments([<<"a">>, <<"..">>, <<"c">>])
        ),
        ?_assertEqual(
            [<<"c">>],
            emqx_utils_uri:remove_dot_segments([<<"..">>, <<"..">>, <<"c">>])
        ),
        %% A segment merely containing dots is kept literally.
        ?_assertEqual(
            [<<"..a">>, <<"b.">>],
            emqx_utils_uri:remove_dot_segments([<<"..a">>, <<"b.">>])
        )
    ].
