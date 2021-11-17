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

-module(emqx_authn_test_lib).

-include("emqx_authn.hrl").

-compile(nowarn_export_all).
-compile(export_all).

authenticator_example(Id) ->
    #{Id := #{value := Example}} = emqx_authn_api:authenticator_examples(),
    Example.

http_example() ->
    authenticator_example('password-based:http').

built_in_database_example() ->
    authenticator_example('password-based:built-in-database').

jwt_example() ->
    authenticator_example(jwt).

delete_config(ID) ->
    {ok, _} =
        emqx:update_config(
            [authentication],
            {delete_authenticator, ?GLOBAL, ID},
            #{rawconf_with_defaults => false}).
