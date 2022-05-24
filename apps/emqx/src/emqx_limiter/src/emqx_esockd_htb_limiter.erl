%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_esockd_htb_limiter).

-behaviour(esockd_generic_limiter).

%% API
-export([new_create_options/2, create/1, delete/1, consume/2]).

-type create_options() :: #{
    module := ?MODULE,
    type := emqx_limiter_schema:limiter_type(),
    bucket := emqx_limiter_schema:bucket_name()
}.

%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------

-spec new_create_options(
    emqx_limiter_schema:limiter_type(),
    emqx_limiter_schema:bucket_name()
) -> create_options().
new_create_options(Type, BucketName) ->
    #{module => ?MODULE, type => Type, bucket => BucketName}.

-spec create(create_options()) -> esockd_generic_limiter:limiter().
create(#{module := ?MODULE, type := Type, bucket := BucketName}) ->
    {ok, Limiter} = emqx_limiter_server:connect(Type, BucketName),
    #{module => ?MODULE, name => Type, limiter => Limiter}.

delete(_GLimiter) ->
    ok.

consume(Token, #{limiter := Limiter} = GLimiter) ->
    case emqx_htb_limiter:check(Token, Limiter) of
        {ok, Limiter2} ->
            {ok, GLimiter#{limiter := Limiter2}};
        {pause, Ms, Retry, Limiter2} ->
            {pause, Ms, GLimiter#{limiter := emqx_htb_limiter:set_retry(Retry, Limiter2)}};
        {drop, Limiter2} ->
            {ok, GLimiter#{limiter := Limiter2}}
    end.

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------
