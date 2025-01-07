%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ft_request_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx_ft, "file_transfer { enable = true, assemble_timeout = 1s}"}
        ],
        #{work_dir => ?config(priv_dir, Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)),
    ok.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%%-------------------------------------------------------------------
%% Tests
%%-------------------------------------------------------------------

t_upload_via_requests(_Config) ->
    C = emqx_ft_test_helpers:start_client(<<"client">>),

    FileId = <<"f1">>,
    Data = <<"hello world">>,
    Size = byte_size(Data),
    Meta = #{
        name => "test.txt",
        expire_at => erlang:system_time(_Unit = second) + 3600,
        size => Size
    },
    MetaPayload = emqx_utils_json:encode(emqx_ft:encode_filemeta(Meta)),
    MetaTopic = <<"$file/", FileId/binary, "/init">>,

    ?assertMatch(
        {ok, #{<<"reason_code">> := 0, <<"topic">> := MetaTopic}},
        request(C, MetaTopic, MetaPayload)
    ),

    SegmentTopic = <<"$file/", FileId/binary, "/0">>,

    ?assertMatch(
        {ok, #{<<"reason_code">> := 0, <<"topic">> := SegmentTopic}},
        request(C, SegmentTopic, Data)
    ),

    FinTopic = <<"$file/", FileId/binary, "/fin/", (integer_to_binary(Size))/binary>>,

    ?assertMatch(
        {ok, #{<<"reason_code">> := 0, <<"topic">> := FinTopic}},
        request(C, FinTopic, <<>>)
    ).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

request(C, Topic, Request) ->
    CorrelaionData = emqx_ft_test_helpers:unique_binary_string(),
    ResponseTopic = emqx_ft_test_helpers:unique_binary_string(),

    Properties = #{
        'Correlation-Data' => CorrelaionData,
        'Response-Topic' => ResponseTopic
    },
    Opts = [{qos, 1}],

    {ok, _, _} = emqtt:subscribe(C, ResponseTopic, 1),
    {ok, _} = emqtt:publish(C, Topic, Properties, Request, Opts),

    try
        receive
            {publish, #{
                topic := ResponseTopic,
                payload := Payload,
                properties := #{'Correlation-Data' := CorrelaionData}
            }} ->
                {ok, emqx_utils_json:decode(Payload)}
        after 1000 ->
            {error, timeout}
        end
    after
        emqtt:unsubscribe(C, ResponseTopic)
    end.
