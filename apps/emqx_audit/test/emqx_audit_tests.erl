%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_audit_tests).

-include_lib("eunit/include/eunit.hrl").

truncate_value_bytes_test() ->
    Maps = #{
        <<"filename">> =>
            #{
                type => <<"application/x-gzip">>,
                <<"file1">> => list_to_binary([$a || _ <- lists:seq(1, 256)])
            }
    },
    #{<<"filename">> := #{<<"file1">> := TruncatedVal}} = emqx_audit:truncate_map_values(Maps),
    ?assertEqual(match, re:run(TruncatedVal, "128 bytes", [{capture, none}])).

truncate_value_depth_test() ->
    BasePath = [<<"a">>, <<"b">>, <<"c">>, <<"d">>],
    Maps1 = emqx_utils_maps:deep_put(BasePath, #{}, #{<<"e">> => <<"f">>}),
    ?assertEqual(
        #{<<"a">> => #{<<"b">> => #{<<"c">> => #{<<"d">> => #{<<"e">> => <<"...">>}}}}},
        emqx_audit:truncate_map_values(Maps1)
    ),

    Maps2 = emqx_utils_maps:deep_put(BasePath, #{}, [#{<<"e">> => <<"f">>}, #{<<"e">> => <<"f">>}]),
    ?assertEqual(
        #{
            <<"a">> => #{
                <<"b">> => #{<<"c">> => #{<<"d">> => [#{<<"e">> => <<"...">>}, <<"...">>]}}
            }
        },
        emqx_audit:truncate_map_values(Maps2)
    ).
