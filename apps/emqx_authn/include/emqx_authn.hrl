%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(APP, emqx_authn).
-define(CHAIN, <<"mqtt">>).

-define(VER_1, <<"1">>).
-define(VER_2, <<"2">>).

-define(RE_PLACEHOLDER, "\\$\\{[a-z0-9\\-]+\\}").

-record(authenticator,
        { id :: binary()
        , name :: binary()
        , provider :: module()
        , config :: map()
        , state :: map()
        }).

-record(chain,
        { id :: binary()
        , authenticators :: [{binary(), binary(), #authenticator{}}]
        , created_at :: integer()
        }).

-define(AUTH_SHARD, emqx_authn_shard).
