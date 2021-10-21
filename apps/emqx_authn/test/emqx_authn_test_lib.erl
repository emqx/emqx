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

-compile(nowarn_export_all).
-compile(export_all).

http_example() ->
"""
{
  mechanism = \"password-based\"
  backend = http
  method = post
  url = \"http://127.0.0.2:8080\"
  headers = {\"content-type\" = \"application/json\"}
  body = {username = \"${username}\",
          password = \"${password}\"}
  pool_size = 8
  connect_timeout = 5000
  request_timeout = 5000
  enable_pipelining = true
  ssl = {enable = false}
}
""".
