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

-type(service_type_name() :: atom()).
-type(service_name() :: binary()).
-type(chain_id() :: binary()).

-record(service_type,
        { name :: service_type_name()
        , provider :: module()
        , params_spec :: #{atom() => term()}
        }).

-record(service,
        { name :: service_name()
        , type :: service_type_name()
        , provider :: module()
        , params :: map()
        , state :: map()
        }).

-record(chain,
        { id :: chain_id()
        , services :: [{service_name(), #service{}}]
        , created_at :: integer()
        }).

-define(AUTH_SHARD, emqx_authentication_shard).
