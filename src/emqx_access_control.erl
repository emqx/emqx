%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_access_control).

-include("emqx.hrl").

-export([authenticate/1]).

-export([ check_acl/3
        , reload_acl/0
        ]).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------
-spec(authenticate(emqx_types:credentials())
      -> {ok, emqx_types:credentials()} | {error, term()}).
authenticate(Credentials) ->
    case emqx_hooks:run_fold('client.authenticate', [], init_auth_result(Credentials)) of
	    #{auth_result := success} = NewCredentials ->
	        {ok, NewCredentials};
	    NewCredentials ->
	        {error, maps:get(auth_result, NewCredentials, unknown_error)}
	end.

%% @doc Check ACL
-spec(check_acl(emqx_types:credentials(), emqx_types:pubsub(), emqx_types:topic()) -> allow | deny).
check_acl(Credentials, PubSub, Topic) ->
    case emqx_acl_cache:is_enabled() of
        false ->
            do_check_acl(Credentials, PubSub, Topic);
        true ->
            case emqx_acl_cache:get_acl_cache(PubSub, Topic) of
                not_found ->
                    AclResult = do_check_acl(Credentials, PubSub, Topic),
                    emqx_acl_cache:put_acl_cache(PubSub, Topic, AclResult),
                    AclResult;
                AclResult ->
                    AclResult
            end
    end.

do_check_acl(#{zone := Zone} = Credentials, PubSub, Topic) ->
    case emqx_hooks:run_fold('client.check_acl', [Credentials, PubSub, Topic],
                             emqx_zone:get_env(Zone, acl_nomatch, deny)) of
        allow -> allow;
        _ -> deny
    end.

-spec(reload_acl() -> ok | {error, term()}).
reload_acl() ->
    emqx_acl_cache:is_enabled() andalso
        emqx_acl_cache:empty_acl_cache(),
    emqx_mod_acl_internal:reload_acl().

init_auth_result(Credentials) ->
    case emqx_zone:get_env(maps:get(zone, Credentials, undefined), allow_anonymous, false) of
	    true -> Credentials#{auth_result => success, anonymous => true};
	    false -> Credentials#{auth_result => not_authorized, anonymous => false}
    end.

