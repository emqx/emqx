%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_auth).
-include_lib("emqx/include/emqx.hrl").

%% API
-export([mnesia/1]).
-boot_mnesia({mnesia, [boot]}).

-export([
    create/4,
    read/1,
    update/4,
    delete/1,
    list/0
]).

-export([authorize/3]).

-define(APP, emqx_app).

-record(?APP, {
    name = <<>> :: binary() | '_',
    api_key = <<>> :: binary() | '_',
    api_secret_hash = <<>> :: binary() | '_',
    enable = true :: boolean() | '_',
    desc = <<>> :: binary() | '_',
    expired_at = 0 :: integer() | undefined | '_',
    created_at = 0 :: integer() | '_'
}).

mnesia(boot) ->
    ok = mria:create_table(?APP, [
        {type, set},
        {rlog_shard, ?COMMON_SHARD},
        {storage, disc_copies},
        {record_name, ?APP},
        {attributes, record_info(fields, ?APP)}
    ]).

create(Name, Enable, ExpiredAt, Desc) ->
    case mnesia:table_info(?APP, size) < 30 of
        true -> create_app(Name, Enable, ExpiredAt, Desc);
        false -> {error, "Maximum ApiKey"}
    end.

read(Name) ->
    Fun = fun() ->
        case mnesia:read(?APP, Name) of
            [] -> mnesia:abort(not_found);
            [App] -> to_map(App)
        end
    end,
    trans(Fun).

update(Name, Enable, ExpiredAt, Desc) ->
    Fun = fun() ->
        case mnesia:read(?APP, Name, write) of
            [] ->
                mnesia:abort(not_found);
            [App0 = #?APP{enable = Enable0, desc = Desc0}] ->
                App =
                    App0#?APP{
                        expired_at = ExpiredAt,
                        enable = ensure_not_undefined(Enable, Enable0),
                        desc = ensure_not_undefined(Desc, Desc0)
                    },
                ok = mnesia:write(App),
                to_map(App)
        end
    end,
    trans(Fun).

delete(Name) ->
    Fun = fun() ->
        case mnesia:read(?APP, Name) of
            [] -> mnesia:abort(not_found);
            [_App] -> mnesia:delete({?APP, Name})
        end
    end,
    trans(Fun).

list() ->
    to_map(ets:match_object(?APP, #?APP{_ = '_'})).

authorize(<<"/api/v5/users", _/binary>>, _ApiKey, _ApiSecret) ->
    {error, <<"not_allowed">>};
authorize(<<"/api/v5/api_key", _/binary>>, _ApiKey, _ApiSecret) ->
    {error, <<"not_allowed">>};
authorize(_Path, ApiKey, ApiSecret) ->
    Now = erlang:system_time(second),
    case find_by_api_key(ApiKey) of
        {ok, true, ExpiredAt, SecretHash} when ExpiredAt >= Now ->
            case emqx_dashboard_admin:verify_hash(ApiSecret, SecretHash) of
                ok -> ok;
                error -> {error, "secret_error"}
            end;
        {ok, true, _ExpiredAt, _SecretHash} ->
            {error, "secret_expired"};
        {ok, false, _ExpiredAt, _SecretHash} ->
            {error, "secret_disable"};
        {error, Reason} ->
            {error, Reason}
    end.

find_by_api_key(ApiKey) ->
    Fun = fun() -> mnesia:match_object(#?APP{api_key = ApiKey, _ = '_'}) end,
    case trans(Fun) of
        {ok, [#?APP{api_secret_hash = SecretHash, enable = Enable, expired_at = ExpiredAt}]} ->
            {ok, Enable, ExpiredAt, SecretHash};
        _ ->
            {error, "not_found"}
    end.

ensure_not_undefined(undefined, Old) -> Old;
ensure_not_undefined(New, _Old) -> New.

to_map(Apps) when is_list(Apps) ->
    [to_map(App) || App <- Apps];
to_map(#?APP{name = N, api_key = K, enable = E, expired_at = ET, created_at = CT, desc = D}) ->
    #{
        name => N,
        api_key => K,
        enable => E,
        expired_at => ET,
        created_at => CT,
        desc => D,
        expired => is_expired(ET)
    }.

is_expired(undefined) -> false;
is_expired(ExpiredTime) -> ExpiredTime < erlang:system_time(second).

create_app(Name, Enable, ExpiredAt, Desc) ->
    ApiSecret = generate_api_secret(),
    App =
        #?APP{
            name = Name,
            enable = Enable,
            expired_at = ExpiredAt,
            desc = Desc,
            created_at = erlang:system_time(second),
            api_secret_hash = emqx_dashboard_admin:hash(ApiSecret),
            api_key = list_to_binary(emqx_misc:gen_id(16))
        },
    case create_app(App) of
        {error, api_key_already_existed} -> create_app(Name, Enable, ExpiredAt, Desc);
        {ok, Res} -> {ok, Res#{api_secret => ApiSecret}};
        Error -> Error
    end.

create_app(App = #?APP{api_key = ApiKey, name = Name}) ->
    trans(fun() ->
        case mnesia:read(?APP, Name) of
            [_] ->
                mnesia:abort(name_already_existed);
            [] ->
                case mnesia:match_object(?APP, #?APP{api_key = ApiKey, _ = '_'}, read) of
                    [] ->
                        ok = mnesia:write(App),
                        to_map(App);
                    _ ->
                        mnesia:abort(api_key_already_existed)
                end
        end
    end).

trans(Fun) ->
    case mria:transaction(?COMMON_SHARD, Fun) of
        {atomic, Res} -> {ok, Res};
        {aborted, Error} -> {error, Error}
    end.

generate_api_secret() ->
    Random = crypto:strong_rand_bytes(32),
    emqx_base62:encode(Random).
