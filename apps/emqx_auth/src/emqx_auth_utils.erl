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

-module(emqx_auth_utils).

%% TODO
%% Move more identical authn and authz helpers here

-export([parse_url/1]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec parse_url(binary()) ->
    {_Base :: emqx_utils_uri:request_base(), _Path :: binary(), _Query :: binary()}.
parse_url(Url) ->
    Parsed = emqx_utils_uri:parse(Url),
    case Parsed of
        #{scheme := undefined} ->
            throw({invalid_url, {no_scheme, Url}});
        #{authority := undefined} ->
            throw({invalid_url, {no_host, Url}});
        #{authority := #{userinfo := Userinfo}} when Userinfo =/= undefined ->
            throw({invalid_url, {userinfo_not_supported, Url}});
        #{fragment := Fragment} when Fragment =/= undefined ->
            throw({invalid_url, {fragments_not_supported, Url}});
        _ ->
            case emqx_utils_uri:request_base(Parsed) of
                {ok, Base} ->
                    {Base, emqx_utils_uri:path(Parsed),
                        emqx_maybe:define(emqx_utils_uri:query(Parsed), <<>>)};
                {error, Reason} ->
                    throw({invalid_url, {invalid_base, Reason, Url}})
            end
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

templates_test_() ->
    [
        ?_assertEqual(
            {
                #{port => 80, scheme => http, host => "example.com"},
                <<"">>,
                <<"client=${clientid}">>
            },
            parse_url(<<"http://example.com?client=${clientid}">>)
        ),
        ?_assertEqual(
            {
                #{port => 80, scheme => http, host => "example.com"},
                <<"/path">>,
                <<"client=${clientid}">>
            },
            parse_url(<<"http://example.com/path?client=${clientid}">>)
        ),
        ?_assertEqual(
            {#{port => 80, scheme => http, host => "example.com"}, <<"/path">>, <<>>},
            parse_url(<<"http://example.com/path">>)
        )
    ].

-endif.
