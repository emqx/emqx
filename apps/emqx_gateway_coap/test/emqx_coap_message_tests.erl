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

-module(emqx_coap_message_tests).

-include_lib("eunit/include/eunit.hrl").

short_name_uri_query_test() ->
    UriQueryMap = #{
        <<"c">> => <<"clientid_value">>,
        <<"t">> => <<"token_value">>,
        <<"u">> => <<"username_value">>,
        <<"p">> => <<"password_value">>,
        <<"q">> => 1,
        <<"r">> => false
    },
    ParsedQuery = #{
        <<"clientid">> => <<"clientid_value">>,
        <<"token">> => <<"token_value">>,
        <<"username">> => <<"username_value">>,
        <<"password">> => <<"password_value">>,
        <<"qos">> => 1,
        <<"retain">> => false
    },
    Msg = emqx_coap_message:request(
        con, put, <<"payload contents...">>, #{uri_query => UriQueryMap}
    ),
    ?assertEqual(ParsedQuery, emqx_coap_message:extract_uri_query(Msg)).
