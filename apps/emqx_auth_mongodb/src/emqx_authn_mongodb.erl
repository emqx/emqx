%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_mongodb).

-behaviour(emqx_authn_provider).

-export([
    create/2,
    update/2,
    authenticate/2,
    destroy/1
]).

-include_lib("emqx_auth/include/emqx_authn.hrl").
-include("emqx_auth_mongodb.hrl").

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(_AuthenticatorID, Config) ->
    create(Config).

create(Config0) ->
    maybe
        ResourceId = emqx_authn_utils:make_resource_id(?AUTHN_BACKEND_BIN),
        {ok, ResourceConfig, State} ?= create_state(ResourceId, Config0),
        ok ?=
            emqx_authn_utils:create_resource(
                emqx_mongodb,
                ResourceConfig,
                State,
                ?AUTHN_MECHANISM_BIN,
                ?AUTHN_BACKEND_BIN
            ),
        {ok, State}
    end.

update(Config0, #{resource_id := ResourceId} = _State) ->
    maybe
        {ok, ResourceConfig, State} ?= create_state(ResourceId, Config0),
        ok ?=
            emqx_authn_utils:update_resource(
                emqx_mongodb,
                ResourceConfig,
                State,
                ?AUTHN_MECHANISM_BIN,
                ?AUTHN_BACKEND_BIN
            ),
        {ok, State}
    end.

destroy(#{resource_id := ResourceId}) ->
    _ = emqx_resource:remove_local(ResourceId),
    ok.

authenticate(#{auth_method := _}, _) ->
    ignore;
authenticate(#{password := undefined}, _) ->
    {error, bad_username_or_password};
authenticate(
    Credential, #{filter_template := FilterTemplate} = State
) ->
    try emqx_auth_template:render_deep_for_json(FilterTemplate, Credential) of
        Filter ->
            authenticate_with_filter(Filter, Credential, State)
    catch
        error:{encode_error, _} = EncodeError ->
            ?TRACE_AUTHN_PROVIDER(error, "mongodb_render_filter_failed", #{
                reason => EncodeError
            }),
            ignore
    end.

authenticate_with_filter(
    Filter,
    #{password := Password} = Credential,
    #{
        collection := Collection,
        resource_id := ResourceId,
        cache_key_template := CacheKeyTemplate
    } = State
) ->
    CacheKey = emqx_auth_template:cache_key(Credential, CacheKeyTemplate),
    Result = emqx_authn_utils:cached_simple_sync_query(
        CacheKey,
        ResourceId,
        {find_one, Collection, Filter}
    ),
    case Result of
        {ok, undefined} ->
            ignore;
        {error, Reason} ->
            ?TRACE_AUTHN_PROVIDER(error, "mongodb_query_failed", #{
                resource => ResourceId,
                collection => Collection,
                filter => Filter,
                reason => Reason
            }),
            ignore;
        {ok, Doc} ->
            case check_password(Password, Doc, State) of
                ok ->
                    {ok, authn_result(Doc, State)};
                {error, {cannot_find_password_hash_field, PasswordHashField}} ->
                    ?TRACE_AUTHN_PROVIDER(error, "cannot_find_password_hash_field", #{
                        resource => ResourceId,
                        collection => Collection,
                        filter => Filter,
                        document => Doc,
                        password_hash_field => PasswordHashField
                    }),
                    ignore;
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

create_state(ResourceId, #{filter := Filter} = Config) ->
    {Vars, FilterTemplate} = emqx_authn_utils:parse_deep(emqx_utils_maps:binary_key_map(Filter)),
    CacheKeyTemplate = emqx_auth_template:cache_key_template(Vars),
    State0 = emqx_authn_utils:init_state(
        Config,
        maps:with(
            [
                collection,
                password_hash_field,
                salt_field,
                is_superuser_field,
                clientid_override_field,
                password_hash_algorithm,
                salt_position
            ],
            Config
        )
    ),
    State = State0#{
        filter_template => FilterTemplate,
        cache_key_template => CacheKeyTemplate,
        resource_id => ResourceId
    },
    ok = emqx_authn_password_hashing:init(maps:get(password_hash_algorithm, State)),
    ResourceConfig = emqx_authn_utils:cleanup_resource_config(
        [
            collection,
            password_hash_field,
            salt_field,
            is_superuser_field,
            password_hash_algorithm,
            salt_position
        ],
        Config
    ),
    {ok, ResourceConfig, State}.

check_password(undefined, _Selected, _State) ->
    {error, bad_username_or_password};
check_password(
    Password,
    Doc,
    #{
        password_hash_algorithm := Algorithm,
        password_hash_field := PasswordHashField
    } = State
) ->
    case maps:get(PasswordHashField, Doc, undefined) of
        undefined ->
            {error, {cannot_find_password_hash_field, PasswordHashField}};
        Hash ->
            Salt =
                case maps:get(salt_field, State, undefined) of
                    undefined -> <<>>;
                    SaltField -> maps:get(SaltField, Doc, <<>>)
                end,
            case emqx_authn_password_hashing:check_password(Algorithm, Salt, Hash, Password) of
                true -> ok;
                false -> {error, bad_username_or_password}
            end
    end.

is_superuser(Doc, #{is_superuser_field := IsSuperuserField}) ->
    IsSuperuser = maps:get(IsSuperuserField, Doc, false),
    emqx_authn_utils:is_superuser(#{<<"is_superuser">> => IsSuperuser});
is_superuser(_, _) ->
    emqx_authn_utils:is_superuser(#{<<"is_superuser">> => false}).

clientid_override(Doc, #{clientid_override_field := ClientIdOverrideField}) ->
    ClientIdOverride = maps:get(ClientIdOverrideField, Doc, undefined),
    emqx_authn_utils:clientid_override(#{<<"clientid_override">> => ClientIdOverride});
clientid_override(_Doc, _State) ->
    #{}.

authn_result(Doc, State) ->
    Res0 = is_superuser(Doc, State),
    Res1 = clientid_override(Doc, State),
    maps:merge(Res0, Res1).
