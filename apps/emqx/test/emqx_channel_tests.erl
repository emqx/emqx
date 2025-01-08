%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_channel_tests).

-include_lib("eunit/include/eunit.hrl").

set_tns_in_log_meta_test_() ->
    PdKey = '$logger_metadata$',
    Original = get(PdKey),
    Set = fun(Cinfo) ->
        Ch = emqx_channel:dummy(),
        Ch1 = emqx_channel:set_field(clientinfo, Cinfo, Ch),
        emqx_channel:set_log_meta(dummy, Ch1)
    end,
    Restore = fun() -> put(PdKey, Original) end,
    NoTns = #{
        clientid => <<"id1">>,
        client_attrs => #{<<"not_tns">> => <<"tns1">>},
        username => <<"user1">>
    },
    NoTnsFn = fun(M) ->
        ?assertMatch(
            #{
                clientid := <<"id1">>,
                username := <<"user1">>
            },
            M
        ),
        ?assertNot(maps:is_key(tns, M))
    end,
    Prefixed = #{
        clientid => <<"tns1-id1">>,
        client_attrs => #{<<"tns">> => <<"tns1">>},
        username => <<"user2">>
    },
    PrefixedFn = fun(M) ->
        ?assertMatch(
            #{
                clientid := <<"tns1-id1">>,
                username := <<"user2">>
            },
            M
        ),
        ?assertNot(maps:is_key(tns, M))
    end,

    Username = #{
        clientid => <<"id1">>,
        client_attrs => #{<<"tns">> => <<"user3">>},
        username => <<"user3">>
    },
    UsernameFn =
        fun(M) ->
            ?assertMatch(
                #{
                    clientid := <<"id1">>,
                    username := <<"user3">>
                },
                M
            ),
            ?assertNot(maps:is_key(tns, M))
        end,
    TnsAdded = #{
        clientid => <<"id4">>,
        client_attrs => #{<<"tns">> => <<"tns1">>},
        username => <<"user4">>
    },
    TnsAddedFn = fun(M) ->
        ?assertMatch(
            #{
                clientid := <<"id4">>,
                username := <<"user4">>,
                tns := <<"tns1">>
            },
            M
        )
    end,
    Run = fun(Cinfo, CheckFn) ->
        Set(Cinfo),
        try
            CheckFn(get(PdKey))
        after
            Restore()
        end
    end,
    MakeTestFn = fun(Cinfo, CheckFn) ->
        fun() ->
            Run(Cinfo, CheckFn)
        end
    end,
    [
        {"tns-added", MakeTestFn(TnsAdded, TnsAddedFn)},
        {"username as tns", MakeTestFn(Username, UsernameFn)},
        {"tns prefixed clientid", MakeTestFn(Prefixed, PrefixedFn)},
        {"no tns", MakeTestFn(NoTns, NoTnsFn)}
    ].
