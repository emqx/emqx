%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_dashboard_audit).

-include_lib("emqx/include/logger.hrl").
%% API
-export([log/1]).

log(Meta0) ->
    #{req_start := ReqStart, req_end := ReqEnd, code := Code, method := Method} = Meta0,
    Duration = erlang:convert_time_unit(ReqEnd - ReqStart, native, millisecond),
    Level = level(Method, Code, Duration),
    Username = maps:get(username, Meta0, <<"">>),
    From = from(maps:get(auth_type, Meta0, "")),
    Meta1 = maps:without([req_start, req_end], Meta0),
    Meta2 = Meta1#{time => logger:timestamp(), duration_ms => Duration},
    Meta = emqx_utils:redact(Meta2),
    ?AUDIT(
        Level,
        From,
        Meta#{username => binary_to_list(Username), node => node()}
    ),
    ok.

from(jwt_token) -> "dashboard";
from(_) -> "rest_api".

level(get, _Code, _) -> debug;
level(_, Code, _) when Code >= 200 andalso Code < 300 -> info;
level(_, Code, _) when Code >= 300 andalso Code < 400 -> warning;
level(_, Code, _) when Code >= 400 andalso Code < 500 -> error;
level(_, _, _) -> critical.
