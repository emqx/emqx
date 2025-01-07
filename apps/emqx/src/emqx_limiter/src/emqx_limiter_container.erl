%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_limiter_container).

%% @doc the container of emqx_htb_limiter
%% used to merge limiters of different type of limiters to simplify operations
%% @end

%% API
-export([
    get_limiter_by_types/3,
    add_new/3,
    set_retry_context/2,
    check/3,
    retry/2,
    get_retry_context/1,
    check_list/2,
    retry_list/2
]).

-export_type([limiter/0, container/0, check_result/0, limiter_type/0]).

-type container() ::
    infinity
    | #{
        limiter_type() => undefined | limiter(),
        %% the retry context of the limiter
        retry_key() =>
            undefined
            | retry_context()
            | future(),
        %% the retry context of the container
        retry_ctx := undefined | any()
    }.

-type future() :: pos_integer().
-type limiter_id() :: emqx_limiter_schema:limiter_id().
-type limiter_type() :: emqx_limiter_schema:limiter_type().
-type limiter() :: emqx_htb_limiter:limiter().
-type retry_context() :: emqx_htb_limiter:retry_context(limiter()).
-type millisecond() :: non_neg_integer().
-type check_result() ::
    {ok, container()}
    | {drop, container()}
    | {pause, millisecond(), container()}.

-define(RETRY_KEY(Type), {retry, Type}).
-type retry_key() :: ?RETRY_KEY(limiter_type()).

%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------
%% @doc generate a container
%% according to the type of limiter and the bucket name configuration of the limiter
%% @end
-spec get_limiter_by_types(
    limiter_id() | {atom(), atom()},
    list(limiter_type()),
    #{limiter_type() => hocon:config()}
) -> container().
get_limiter_by_types({Type, Listener}, Types, BucketCfgs) ->
    Id = emqx_listeners:listener_id(Type, Listener),
    get_limiter_by_types(Id, Types, BucketCfgs);
get_limiter_by_types(Id, Types, BucketCfgs) ->
    Init = fun(Type, Acc) ->
        {ok, Limiter} = emqx_limiter_server:connect(Id, Type, BucketCfgs),
        add_new(Type, Limiter, Acc)
    end,
    Container = lists:foldl(Init, #{retry_ctx => undefined}, Types),
    case
        lists:all(
            fun(Type) ->
                maps:get(Type, Container) =:= infinity
            end,
            Types
        )
    of
        true ->
            infinity;
        _ ->
            Container
    end.

-spec add_new(limiter_type(), limiter(), container()) -> container().
add_new(Type, Limiter, Container) ->
    Container#{
        Type => Limiter,
        ?RETRY_KEY(Type) => undefined
    }.

%% @doc check the specified limiter
-spec check(pos_integer(), limiter_type(), container()) -> check_result().
check(_Need, _Type, infinity) ->
    {ok, infinity};
check(Need, Type, Container) ->
    check_list([{Need, Type}], Container).

%% @doc check multiple limiters
-spec check_list(list({pos_integer(), limiter_type()}), container()) -> check_result().
check_list(_Need, infinity) ->
    {ok, infinity};
check_list([{Need, Type} | T], Container) ->
    Limiter = maps:get(Type, Container),
    case emqx_htb_limiter:check(Need, Limiter) of
        {ok, Limiter2} ->
            check_list(T, Container#{Type := Limiter2});
        {_, PauseMs, Ctx, Limiter2} ->
            Fun = fun({FN, FT}, Acc) ->
                Future = emqx_htb_limiter:make_future(FN),
                Acc#{?RETRY_KEY(FT) := Future}
            end,
            C2 = lists:foldl(
                Fun,
                Container#{
                    Type := Limiter2,
                    ?RETRY_KEY(Type) := Ctx
                },
                T
            ),
            {pause, PauseMs, C2};
        {drop, Limiter2} ->
            {drop, Container#{Type := Limiter2}}
    end;
check_list([], Container) ->
    {ok, Container}.

%% @doc retry the specified limiter
-spec retry(limiter_type(), container()) -> check_result().
retry(_Type, infinity) ->
    {ok, infinity};
retry(Type, Container) ->
    retry_list([Type], Container).

%% @doc retry multiple limiters
-spec retry_list(list(limiter_type()), container()) -> check_result().
retry_list(_Types, infinity) ->
    {ok, infinity};
retry_list([Type | T], Container) ->
    Key = ?RETRY_KEY(Type),
    case Container of
        #{
            Type := Limiter,
            Key := Retry
        } when Retry =/= undefined ->
            case emqx_htb_limiter:check(Retry, Limiter) of
                {ok, Limiter2} ->
                    %% undefined meaning there is no retry context or there is no need to retry
                    %% when a limiter has a undefined retry context, the check will always success
                    retry_list(T, Container#{Type := Limiter2, Key := undefined});
                {_, PauseMs, Ctx, Limiter2} ->
                    {pause, PauseMs, Container#{Type := Limiter2, Key := Ctx}};
                {drop, Limiter2} ->
                    {drop, Container#{Type := Limiter2}}
            end;
        _ ->
            retry_list(T, Container)
    end;
retry_list([], Container) ->
    {ok, Container}.

-spec set_retry_context(any(), container()) -> container().
set_retry_context(Data, Container) ->
    Container#{retry_ctx := Data}.

-spec get_retry_context(container()) -> any().
get_retry_context(#{retry_ctx := Data}) ->
    Data.

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------
