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

-module(emqx_authz_test_lib).

-include("emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(nowarn_export_all).
-compile(export_all).

-define(DEFAULT_CHECK_AVAIL_TIMEOUT, 1000).

reset_authorizers() ->
    reset_authorizers(deny, false).

restore_authorizers() ->
    reset_authorizers(allow, true).

reset_authorizers(Nomatch, ChacheEnabled) ->
    {ok, _} = emqx:update_config(
        [authorization],
        #{
            <<"no_match">> => atom_to_binary(Nomatch),
            <<"cache">> => #{<<"enable">> => atom_to_binary(ChacheEnabled)},
            <<"sources">> => []
        }
    ),
    ok.

setup_config(BaseConfig, SpecialParams) ->
    Config = maps:merge(BaseConfig, SpecialParams),
    case emqx_authz:update(?CMD_REPLACE, [Config]) of
        {ok, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.

test_samples(ClientInfo, Samples) ->
    lists:foreach(
        fun({Expected, Action, Topic}) ->
            ct:pal(
                "client_info: ~p, action: ~p, topic: ~p, expected: ~p",
                [ClientInfo, Action, Topic, Expected]
            ),
            ?assertEqual(
                Expected,
                emqx_access_control:authorize(
                    ClientInfo,
                    Action,
                    Topic
                )
            )
        end,
        Samples
    ).

test_no_topic_rules(ClientInfo, SetupSamples) ->
    %% No rules

    ok = reset_authorizers(deny, false),
    ok = SetupSamples(ClientInfo, []),

    ok = test_samples(
        ClientInfo,
        [
            {deny, subscribe, <<"#">>},
            {deny, subscribe, <<"subs">>},
            {deny, publish, <<"pub">>}
        ]
    ).

test_allow_topic_rules(ClientInfo, SetupSamples) ->
    Samples = [
        #{
            topics => [
                <<"eq testpub1/${username}">>,
                <<"testpub2/${clientid}">>,
                <<"testpub3/#">>
            ],
            permission => <<"allow">>,
            action => <<"publish">>
        },
        #{
            topics => [
                <<"eq testsub1/${username}">>,
                <<"testsub2/${clientid}">>,
                <<"testsub3/#">>
            ],
            permission => <<"allow">>,
            action => <<"subscribe">>
        },

        #{
            topics => [
                <<"eq testall1/${username}">>,
                <<"testall2/${clientid}">>,
                <<"testall3/#">>
            ],
            permission => <<"allow">>,
            action => <<"all">>
        }
    ],

    ok = reset_authorizers(deny, false),
    ok = SetupSamples(ClientInfo, Samples),

    ok = test_samples(
        ClientInfo,
        [
            %% Publish rules

            {deny, publish, <<"testpub1/username">>},
            {allow, publish, <<"testpub1/${username}">>},
            {allow, publish, <<"testpub2/clientid">>},
            {allow, publish, <<"testpub3/foobar">>},

            {deny, publish, <<"testpub2/username">>},
            {deny, publish, <<"testpub1/clientid">>},

            {deny, subscribe, <<"testpub1/username">>},
            {deny, subscribe, <<"testpub2/clientid">>},
            {deny, subscribe, <<"testpub3/foobar">>},

            %% Subscribe rules

            {deny, subscribe, <<"testsub1/username">>},
            {allow, subscribe, <<"testsub1/${username}">>},
            {allow, subscribe, <<"testsub2/clientid">>},
            {allow, subscribe, <<"testsub3/foobar">>},
            {allow, subscribe, <<"testsub3/+/foobar">>},
            {allow, subscribe, <<"testsub3/#">>},

            {deny, subscribe, <<"testsub2/username">>},
            {deny, subscribe, <<"testsub1/clientid">>},
            {deny, subscribe, <<"testsub4/foobar">>},
            {deny, publish, <<"testsub1/username">>},
            {deny, publish, <<"testsub2/clientid">>},
            {deny, publish, <<"testsub3/foobar">>},

            %% All rules

            {deny, subscribe, <<"testall1/username">>},
            {allow, subscribe, <<"testall1/${username}">>},
            {allow, subscribe, <<"testall2/clientid">>},
            {allow, subscribe, <<"testall3/foobar">>},
            {allow, subscribe, <<"testall3/+/foobar">>},
            {allow, subscribe, <<"testall3/#">>},
            {deny, publish, <<"testall1/username">>},
            {allow, publish, <<"testall1/${username}">>},
            {allow, publish, <<"testall2/clientid">>},
            {allow, publish, <<"testall3/foobar">>},

            {deny, subscribe, <<"testall2/username">>},
            {deny, subscribe, <<"testall1/clientid">>},
            {deny, subscribe, <<"testall4/foobar">>},
            {deny, publish, <<"testall2/username">>},
            {deny, publish, <<"testall1/clientid">>},
            {deny, publish, <<"testall4/foobar">>}
        ]
    ).

test_deny_topic_rules(ClientInfo, SetupSamples) ->
    Samples = [
        #{
            topics => [
                <<"eq testpub1/${username}">>,
                <<"testpub2/${clientid}">>,
                <<"testpub3/#">>
            ],
            permission => <<"deny">>,
            action => <<"publish">>
        },
        #{
            topics => [
                <<"eq testsub1/${username}">>,
                <<"testsub2/${clientid}">>,
                <<"testsub3/#">>
            ],
            permission => <<"deny">>,
            action => <<"subscribe">>
        },

        #{
            topics => [
                <<"eq testall1/${username}">>,
                <<"testall2/${clientid}">>,
                <<"testall3/#">>
            ],
            permission => <<"deny">>,
            action => <<"all">>
        }
    ],

    ok = reset_authorizers(allow, false),
    ok = SetupSamples(ClientInfo, Samples),

    ok = test_samples(
        ClientInfo,
        [
            %% Publish rules

            {allow, publish, <<"testpub1/username">>},
            {deny, publish, <<"testpub1/${username}">>},
            {deny, publish, <<"testpub2/clientid">>},
            {deny, publish, <<"testpub3/foobar">>},

            {allow, publish, <<"testpub2/username">>},
            {allow, publish, <<"testpub1/clientid">>},

            {allow, subscribe, <<"testpub1/username">>},
            {allow, subscribe, <<"testpub2/clientid">>},
            {allow, subscribe, <<"testpub3/foobar">>},

            %% Subscribe rules

            {allow, subscribe, <<"testsub1/username">>},
            {deny, subscribe, <<"testsub1/${username}">>},
            {deny, subscribe, <<"testsub2/clientid">>},
            {deny, subscribe, <<"testsub3/foobar">>},
            {deny, subscribe, <<"testsub3/+/foobar">>},
            {deny, subscribe, <<"testsub3/#">>},

            {allow, subscribe, <<"testsub2/username">>},
            {allow, subscribe, <<"testsub1/clientid">>},
            {allow, subscribe, <<"testsub4/foobar">>},
            {allow, publish, <<"testsub1/username">>},
            {allow, publish, <<"testsub2/clientid">>},
            {allow, publish, <<"testsub3/foobar">>},

            %% All rules

            {allow, subscribe, <<"testall1/username">>},
            {deny, subscribe, <<"testall1/${username}">>},
            {deny, subscribe, <<"testall2/clientid">>},
            {deny, subscribe, <<"testall3/foobar">>},
            {deny, subscribe, <<"testall3/+/foobar">>},
            {deny, subscribe, <<"testall3/#">>},
            {allow, publish, <<"testall1/username">>},
            {deny, publish, <<"testall1/${username}">>},
            {deny, publish, <<"testall2/clientid">>},
            {deny, publish, <<"testall3/foobar">>},

            {allow, subscribe, <<"testall2/username">>},
            {allow, subscribe, <<"testall1/clientid">>},
            {allow, subscribe, <<"testall4/foobar">>},
            {allow, publish, <<"testall2/username">>},
            {allow, publish, <<"testall1/clientid">>},
            {allow, publish, <<"testall4/foobar">>}
        ]
    ).
