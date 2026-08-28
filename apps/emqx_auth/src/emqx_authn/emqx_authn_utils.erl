%%--------------------------------------------------------------------
%% Copyright (c) 2021-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_utils).

-include("emqx_authn.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([
    create_resource/5,
    update_resource/5,
    init_state/2,
    cleanup_resource_config/2,
    check_password_from_selected_map/3,
    parse_deep/1,
    parse_str/1,
    parse_sql/2,
    is_superuser/1,
    client_attrs/1,
    maybe_client_attrs/1,
    clientid_override/1,
    bin/1,
    ensure_apps_started/1,
    cleanup_resources/0,
    make_resource_id/1,
    without_password/1,
    to_bool/1,
    backend_failure_result/0,
    authn_backend_failure_policy/0,
    cached_simple_sync_query/3,
    cached_apply/2
]).

-define(DEFAULT_RESOURCE_OPTS(OWNER_ID), #{
    start_after_created => false,
    spawn_buffer_workers => false,
    owner_id => OWNER_ID
}).

-type authn_provider_state() :: #{resource_id => binary(), enable => boolean(), _ => _}.
-type resource_config() :: map().
-type mechanism() :: atom() | binary().
-type backend() :: atom() | binary().
-type template_var() :: emqx_template:varname() | {var_namespace, emqx_template:varname()}.
-type used_template_vars() :: [template_var()].

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec create_resource(module(), resource_config(), authn_provider_state(), mechanism(), backend()) ->
    ok | {error, term()}.
create_resource(Module, ResourceConfig, #{resource_id := ResourceId} = State, Mechanism, Backend) ->
    maybe
        OwnerId = owner_id(Mechanism, Backend),
        {ok, _} ?=
            emqx_resource:create_local(
                ResourceId,
                ?AUTHN_RESOURCE_GROUP,
                Module,
                ResourceConfig,
                ?DEFAULT_RESOURCE_OPTS(OwnerId)
            ),
        ok =
            remove_resource_on_exception(
                ResourceId,
                fun() -> start_resource_if_enabled(State, Mechanism, Backend) end
            )
    end.

-spec update_resource(module(), resource_config(), authn_provider_state(), mechanism(), backend()) ->
    ok | {error, term()}.
update_resource(Module, ResourceConfig, #{resource_id := ResourceId} = State, Mechanism, Backend) ->
    maybe
        OwnerId = owner_id(Mechanism, Backend),
        {ok, _} ?=
            emqx_resource:recreate_local(
                ResourceId, Module, ResourceConfig, ?DEFAULT_RESOURCE_OPTS(OwnerId)
            ),
        start_resource_if_enabled(State, Mechanism, Backend)
    end.

-spec start_resource_if_enabled(authn_provider_state(), mechanism(), backend()) -> ok.
start_resource_if_enabled(#{resource_id := ResourceId, enable := true}, Mechanism, Backend) ->
    case emqx_resource:start(ResourceId) of
        ok ->
            ok;
        timeout ->
            handle_start_resource_error(ResourceId, timeout, Mechanism, Backend);
        {error, Reason} ->
            handle_start_resource_error(ResourceId, Reason, Mechanism, Backend)
    end;
start_resource_if_enabled(#{resource_id := _ResourceId, enable := false}, _Mechanism, _Backend) ->
    ok.

handle_start_resource_error(ResourceId, Reason, Mechanism, Backend) ->
    %% NOTE
    %% we allow creation of resources that cannot be started
    ?SLOG(warning, #{
        msg => "failed_to_start_authn_resource",
        resource_id => ResourceId,
        reason => Reason,
        mechanism => Mechanism,
        backend => Backend
    }),
    ok.

remove_resource_on_exception(ResourceId, Operation) ->
    try
        Operation()
    catch
        Class:Reason:Stacktrace ->
            _ = cleanup_created_resource(ResourceId),
            erlang:raise(Class, Reason, Stacktrace)
    end.

cleanup_created_resource(ResourceId) ->
    try
        emqx_resource:remove_local(ResourceId)
    catch
        _:_ -> ok
    end.

-spec init_state(map(), map()) -> authn_provider_state().
init_state(#{enable := Enable} = _Source, Values) ->
    maps:merge(
        #{
            enable => Enable
        },
        Values
    ).

-spec cleanup_resource_config(list(atom()), resource_config()) -> resource_config().
cleanup_resource_config(WithoutFields, Config) ->
    maps:without([enable] ++ WithoutFields, Config).

-spec parse_deep(term()) -> {used_template_vars(), emqx_template:t()}.
parse_deep(Template) -> emqx_auth_template:parse_deep(Template, ?AUTHN_DEFAULT_ALLOWED_VARS).

-spec parse_str(unicode:chardata()) -> {used_template_vars(), emqx_template:t()}.
parse_str(Template) -> emqx_auth_template:parse_str(Template, ?AUTHN_DEFAULT_ALLOWED_VARS).

-spec parse_sql(emqx_template_sql:raw_statement_template(), emqx_template_sql:sql_parameters()) ->
    {used_template_vars(), emqx_template_sql:statement(), emqx_template_sql:row_template()}.
parse_sql(Template, ReplaceWith) ->
    emqx_auth_template:parse_sql(Template, ReplaceWith, ?AUTHN_DEFAULT_ALLOWED_VARS).

-spec check_password_from_selected_map(atom(), #{binary() => term()}, binary() | undefined) ->
    {error, bad_username_or_password} | ok.
check_password_from_selected_map(_Algorithm, _Selected, undefined) ->
    {error, bad_username_or_password};
check_password_from_selected_map(Algorithm, Selected, Password) ->
    Hash = maps:get(
        <<"password_hash">>,
        Selected,
        maps:get(<<"password">>, Selected, undefined)
    ),
    case Hash of
        undefined ->
            {error, not_authorized};
        _ ->
            Salt = maps:get(<<"salt">>, Selected, <<>>),
            case
                emqx_authn_password_hashing:check_password(
                    Algorithm, Salt, Hash, Password
                )
            of
                true -> ok;
                false -> {error, bad_username_or_password}
            end
    end.

-spec is_superuser(#{binary() => term()}) -> #{is_superuser => boolean()}.
is_superuser(#{<<"is_superuser">> := Value}) ->
    #{is_superuser => to_bool(Value)};
is_superuser(#{}) ->
    #{is_superuser => false}.

%% @doc Collect client attributes from a backend result.
%%
%% Two shapes are accepted, so that a backend which returns a whole map (an
%% HTTP JSON body, a Mongo subdocument) and one which can only return flat
%% columns (SQL, a Redis hash) both have a way to express them:
%%
%%   * `client_attrs'          - a map of attributes;
%%   * `client_attrs.<name>'   - one attribute per key, which is what a SQL
%%                               query aliases a column to.
%%
%% A per-attribute key wins over the same name inside the map, so a query can
%% override one attribute without rebuilding the whole thing.
-spec client_attrs(#{binary() => term()}) -> #{client_attrs => map()}.
client_attrs(Selected) when is_map(Selected) ->
    Attrs = maps:merge(
        attr_map(maps:get(<<"client_attrs">>, Selected, #{})),
        prefixed_attrs(Selected)
    ),
    #{client_attrs => drop_invalid_attr(Attrs)};
client_attrs(_) ->
    #{client_attrs => #{}}.

%% @doc Like `client_attrs/1', but leaves the key out when the backend returned
%% no attributes, so a result keeps exactly the shape it had before a backend
%% learned to report them. `clientid_override/1' omits its key the same way.
-spec maybe_client_attrs(#{binary() => term()}) -> #{client_attrs => map()}.
maybe_client_attrs(Selected) ->
    case client_attrs(Selected) of
        #{client_attrs := Attrs} = Result when map_size(Attrs) > 0 ->
            Result;
        _ ->
            #{}
    end.

attr_map(Attrs) when is_map(Attrs) -> Attrs;
attr_map(_NotAMap) -> #{}.

prefixed_attrs(Selected) ->
    maps:fold(
        fun
            (<<"client_attrs.", Name/binary>>, Value, Acc) ->
                Acc#{Name => Value};
            (_Key, _Value, Acc) ->
                Acc
        end,
        #{},
        Selected
    ).

-spec clientid_override(#{binary() => term()}) -> #{clientid_override => term()}.
clientid_override(#{<<"clientid_override">> := Value}) when
    is_binary(Value) andalso Value /= <<"">>
->
    #{clientid_override => Value};
clientid_override(_) ->
    #{}.

-spec ensure_apps_started(term()) -> ok.
ensure_apps_started(bcrypt) ->
    {ok, _} = application:ensure_all_started(bcrypt),
    ok;
ensure_apps_started(_) ->
    ok.

-spec bin(atom() | binary() | list()) -> binary().
bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(L) when is_list(L) -> iolist_to_binary(L);
bin(X) when is_binary(X) -> X.

-spec cleanup_resources() -> ok.
cleanup_resources() ->
    lists:foreach(
        fun emqx_resource:remove_local/1,
        emqx_resource:list_group_instances(?AUTHN_RESOURCE_GROUP)
    ).

-spec make_resource_id(term()) -> emqx_resource:resource_id().
make_resource_id(Name) ->
    NameBin = bin([<<"authn:">>, bin(Name)]),
    emqx_resource:generate_id(NameBin).

-spec without_password(map()) -> map().
without_password(Credential) ->
    without_password(Credential, [password, <<"password">>]).

-spec to_bool(term()) -> boolean().
to_bool(<<"true">>) ->
    true;
to_bool(true) ->
    true;
to_bool(<<"1">>) ->
    true;
to_bool(I) when is_integer(I) andalso I >= 1 ->
    true;
%% false
to_bool(<<"">>) ->
    false;
to_bool(<<"0">>) ->
    false;
to_bool(0) ->
    false;
to_bool(null) ->
    false;
to_bool(undefined) ->
    false;
to_bool(<<"false">>) ->
    false;
to_bool(false) ->
    false;
to_bool(MaybeBinInt) when is_binary(MaybeBinInt) ->
    try
        binary_to_integer(MaybeBinInt) >= 1
    catch
        error:badarg ->
            false
    end;
%% fallback to default
to_bool(_) ->
    false.

-spec cached_simple_sync_query(
    emqx_auth_cache:cache_key(),
    emqx_resource:resource_id(),
    _Request :: term()
) -> term().
cached_simple_sync_query(CacheKey, ResourceID, Query) ->
    emqx_auth_utils:cached_simple_sync_query(?AUTHN_CACHE, CacheKey, ResourceID, Query).

-spec cached_apply(emqx_auth_cache:cache_key(), fun(() -> term())) -> term().
cached_apply(CacheKey, Fun) ->
    emqx_auth_utils:cached_apply(?AUTHN_CACHE, CacheKey, Fun).

-spec backend_failure_result() -> ignore | {error, not_authorized}.
backend_failure_result() ->
    case authn_backend_failure_policy() of
        ignore -> ignore;
        deny -> {error, not_authorized}
    end.

-spec authn_backend_failure_policy() -> ignore | deny.
authn_backend_failure_policy() ->
    case emqx_security_profile:policy(authn_backend_failure) of
        deny ->
            case emqx:get_config([authentication_settings, ignore_backend_failures], false) of
                true -> ignore;
                false -> deny
            end;
        ignore ->
            ignore
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

without_password(Credential, []) ->
    Credential;
without_password(Credential, [Name | Rest]) ->
    case maps:is_key(Name, Credential) of
        true ->
            without_password(Credential#{Name => <<"[password]">>}, Rest);
        false ->
            without_password(Credential, Rest)
    end.

owner_id(Mechanism, Backend) ->
    bin([bin(Mechanism), ":", bin(Backend)]).

drop_invalid_attr(Map) when is_map(Map) ->
    maps:from_list(do_drop_invalid_attr(maps:to_list(Map))).

do_drop_invalid_attr([]) ->
    [];
do_drop_invalid_attr([{K, V} | More]) ->
    case emqx_utils:is_restricted_str(K) of
        true ->
            case attr_value(V) of
                {ok, Value} ->
                    [{iolist_to_binary(K), Value} | do_drop_invalid_attr(More)];
                error ->
                    ?SLOG(debug, #{msg => "invalid_client_attr_value_dropped", attr_name => K}, #{
                        tag => "AUTHN"
                    }),
                    do_drop_invalid_attr(More)
            end;
        false ->
            ?SLOG(debug, #{msg => "invalid_client_attr_dropped", attr_name => K}, #{
                tag => "AUTHN"
            }),
            do_drop_invalid_attr(More)
    end.

%% A client attribute value is a binary. Database columns are typed, so one can
%% arrive as a number or a boolean, and a nullable column arrives as `null' or
%% `undefined'. Convert what has an obvious representation and drop the rest:
%% `iolist_to_binary/1' on, say, an integer raises badarg, which
%% `emqx_authn_chains' turns into an `authenticator_error' and the client is
%% refused - a whole failed login over one unusable attribute.
attr_value(V) when is_binary(V) -> {ok, V};
attr_value(V) when is_integer(V) -> {ok, integer_to_binary(V)};
attr_value(V) when is_float(V) -> {ok, float_to_binary(V, [short])};
attr_value(true) ->
    {ok, <<"true">>};
attr_value(false) ->
    {ok, <<"false">>};
attr_value(V) when is_list(V) ->
    try
        {ok, iolist_to_binary(V)}
    catch
        _:_ -> error
    end;
attr_value(_Other) ->
    error.
