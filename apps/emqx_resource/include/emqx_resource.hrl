%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-type resource_type() :: module().
-type instance_id() :: binary().
-type resource_config() :: term().
-type resource_spec() :: map().
-type resource_state() :: term().
-type resource_data() :: #{
    id => instance_id(),
    mod => module(),
    config => resource_config(),
    state => resource_state(),
    status => started | stopped
}.

-type after_query() :: {OnSuccess :: after_query_fun(), OnFailed :: after_query_fun()} |
    undefined.

%% the `after_query_fun()` is mainly for callbacks that increment counters or do some fallback
%% actions upon query failure
-type after_query_fun() :: {fun((...) -> ok), Args :: [term()]}.
