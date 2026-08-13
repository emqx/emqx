%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_mnesia_schema).

-include("emqx_auth_mnesia.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(emqx_authn_schema).

-export([
    fields/1,
    desc/1,
    refs/1,
    root_converter/1,
    select_union_member/2,
    namespace/0,
    default_bootstrap_file_path/0
]).

namespace() -> "authn".

refs(api_write) ->
    [?R_REF(builtin_db_generated_api), ?R_REF(builtin_db_manual_api)];
refs(_) ->
    [?R_REF(builtin_db_generated), ?R_REF(builtin_db_manual)].

select_union_member(
    Kind,
    #{
        <<"mechanism">> := ?AUTHN_MECHANISM_SIMPLE_BIN, <<"backend">> := ?AUTHN_BACKEND_BIN
    } = Value
) ->
    builtin_db_refs(Kind, autogenerate_password(Value));
select_union_member(_Kind, _Value) ->
    undefined.

fields(builtin_db_generated) ->
    [
        {autogenerate_password,
            hoconsc:mk(true, #{
                required => true,
                default => true,
                desc => ?DESC(autogenerate_password)
            })},
        {password_hash_algorithm, fun emqx_authn_password_hashing:type_builtin_generated/1}
    ] ++ common_fields();
fields(builtin_db_generated_api) ->
    fields(builtin_db_generated);
fields(builtin_db_manual) ->
    [
        {autogenerate_password,
            hoconsc:mk(false, #{
                required => true,
                default => false,
                desc => ?DESC(autogenerate_password)
            })},
        {password_hash_algorithm, fun emqx_authn_password_hashing:type_builtin_rw/1}
    ] ++ common_fields();
fields(builtin_db_manual_api) ->
    [
        {autogenerate_password,
            hoconsc:mk(false, #{
                required => true,
                default => false,
                desc => ?DESC(autogenerate_password)
            })},
        {password_hash_algorithm, fun emqx_authn_password_hashing:type_builtin_rw_api/1}
    ] ++ common_fields().

root_converter(Name) when
    Name =:= builtin_db_generated;
    Name =:= builtin_db_generated_api;
    Name =:= builtin_db_manual;
    Name =:= builtin_db_manual_api
->
    fun builtin_db_converter/2;
root_converter(_) ->
    undefined.

desc(Name) when
    Name =:= builtin_db_generated;
    Name =:= builtin_db_generated_api;
    Name =:= builtin_db_manual;
    Name =:= builtin_db_manual_api
->
    ?DESC(builtin_db);
desc(_) ->
    undefined.

user_id_type(type) -> hoconsc:enum([clientid, username]);
user_id_type(desc) -> ?DESC(?FUNCTION_NAME);
user_id_type(default) -> <<"username">>;
user_id_type(required) -> true;
user_id_type(_) -> undefined.

common_fields() ->
    [
        {mechanism, emqx_authn_schema:mechanism(?AUTHN_MECHANISM_SIMPLE)},
        {backend, emqx_authn_schema:backend(?AUTHN_BACKEND)},
        {user_id_type, fun user_id_type/1}
    ] ++ bootstrap_fields() ++
        emqx_authn_schema:common_fields().

bootstrap_fields() ->
    [
        {bootstrap_file,
            ?HOCON(
                binary(),
                #{
                    desc => ?DESC(bootstrap_file),
                    required => false,
                    default => default_bootstrap_file_path()
                }
            )},
        {bootstrap_type,
            ?HOCON(
                ?ENUM([hash, plain]), #{
                    desc => ?DESC(bootstrap_type),
                    required => false,
                    default => <<"plain">>
                }
            )}
    ].

default_bootstrap_file_path() ->
    <<"${EMQX_ETC_DIR}/auth-built-in-db-bootstrap.csv">>.

builtin_db_refs(api_write, true) -> [?R_REF(builtin_db_generated_api)];
builtin_db_refs(api_write, false) -> [?R_REF(builtin_db_manual_api)];
builtin_db_refs(api_write, _) -> refs(api_write);
builtin_db_refs(_, true) -> [?R_REF(builtin_db_generated)];
builtin_db_refs(_, false) -> [?R_REF(builtin_db_manual)];
builtin_db_refs(Kind, _) -> refs(Kind).

autogenerate_password(Value) ->
    maps:get(
        <<"autogenerate_password">>,
        Value,
        emqx_security_profile:policy(authn_builtin_default_autogenerate_password)
    ).

%% We need different defaults for hash when autogenerate_password (a sibling field)
%% is enabled vs disabled.
%% This is only possible to do in the whole struct converter.
builtin_db_converter(undefined, _Opts) ->
    undefined;
builtin_db_converter(Conf, _Opts) when map_size(Conf) =:= 0 ->
    Conf;
builtin_db_converter(Conf0, _Opts) when is_map(Conf0) ->
    case Conf0 of
        #{<<"password_hash_algorithm">> := _} ->
            Conf0;
        #{<<"autogenerate_password">> := Autogenerate} when is_boolean(Autogenerate) ->
            Conf0#{<<"password_hash_algorithm">> => default_password_hash_algorithm(Autogenerate)};
        #{<<"autogenerate_password">> := _} ->
            Conf0;
        #{} ->
            Autogenerate =
                emqx_security_profile:policy(authn_builtin_default_autogenerate_password),
            Conf0#{<<"password_hash_algorithm">> => default_password_hash_algorithm(Autogenerate)}
    end;
builtin_db_converter(Conf, _Opts) ->
    Conf.

default_password_hash_algorithm(true) ->
    #{<<"name">> => <<"sha256">>};
default_password_hash_algorithm(false) ->
    HashName = emqx_security_profile:policy(authn_builtin_default_manual_password_hash),
    #{<<"name">> => atom_to_binary(HashName, utf8)}.
