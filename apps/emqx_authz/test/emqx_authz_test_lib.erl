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

-module(emqx_authz_test_lib).

-include("emqx_authz.hrl").

-compile(nowarn_export_all).
-compile(export_all).

-define(DEFAULT_CHECK_AVAIL_TIMEOUT, 1000).

reset_authorizers() ->
    reset_authorizers(allow, true).

reset_authorizers(Nomatch, ChacheEnabled) ->
    {ok, _} = emqx:update_config(
                [authorization],
                #{<<"no_match">> => atom_to_binary(Nomatch),
                  <<"cache">> => #{<<"enable">> => atom_to_binary(ChacheEnabled)},
                  <<"sources">> => []}),
    ok.

is_tcp_server_available(Host, Port) ->
    case gen_tcp:connect(Host, Port, [], ?DEFAULT_CHECK_AVAIL_TIMEOUT) of
        {ok, Socket} ->
            gen_tcp:close(Socket),
            true;
        {error, _} ->
            false
    end.
