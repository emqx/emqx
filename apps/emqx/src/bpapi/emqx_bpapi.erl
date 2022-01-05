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
-module(emqx_bpapi).

-export_type([var_name/0, call/0, rpc/0, bpapi_meta/0]).

-type api() :: atom().
-type api_version() :: non_neg_integer().
-type var_name() :: atom().
-type call() :: {module(), atom(), [var_name()]}.
-type rpc() :: {_From :: call(), _To :: call()}.

-type bpapi_meta() ::
        #{ api     := api()
         , version := api_version()
         , calls   := [rpc()]
         , casts   := [rpc()]
         }.

-callback introduced_in() -> string().

-callback deprecated_since() -> string().

-callback bpapi_meta() -> bpapi_meta().

-optional_callbacks([deprecated_since/0]).
