%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_gcp_pubsub_schema_lib).

%% API
-export([
    authentication_field/0
]).
-export([
    legacy_service_account_json_root_converter/2,
    service_account_json_validator/1
]).

%% `hocon_schema` API
-export([
    namespace/0,
    fields/1,
    desc/1
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include_lib("hocon/include/hoconsc.hrl").

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

authentication_field() ->
    {authentication,
        mk(
            emqx_schema:mkunion(type, #{
                <<"service_account_json">> => hoconsc:ref(?MODULE, auth_service_account_json)
            }),
            #{
                required => true,
                desc => ?DESC(authentication)
            }
        )}.

-spec service_account_json_validator(binary()) ->
    ok
    | {error, {wrong_type, term()}}
    | {error, {missing_keys, [binary()]}}.
service_account_json_validator(Val) ->
    case emqx_utils_json:safe_decode(Val) of
        {ok, Map} ->
            ExpectedKeys = [
                <<"type">>,
                <<"project_id">>,
                <<"private_key_id">>,
                <<"private_key">>,
                <<"client_email">>
            ],
            MissingKeys = lists:sort([
                K
             || K <- ExpectedKeys,
                not maps:is_key(K, Map)
            ]),
            Type = maps:get(<<"type">>, Map, null),
            case {MissingKeys, Type} of
                {[], <<"service_account">>} ->
                    ok;
                {[], Type} ->
                    {error, #{wrong_type => Type}};
                {_, _} ->
                    {error, #{missing_keys => MissingKeys}}
            end;
        {error, _} ->
            {error, "not a json"}
    end.

legacy_service_account_json_root_converter(Conf0, _HoconOpts) ->
    case Conf0 of
        #{<<"service_account_json">> := SAJ} ->
            Conf = maps:remove(<<"service_account_json">>, Conf0),
            Conf#{
                <<"authentication">> => #{
                    <<"type">> => <<"service_account_json">>,
                    <<"service_account_json">> => SAJ
                }
            };
        _ ->
            Conf0
    end.

%%------------------------------------------------------------------------------
%% `hocon_schema` API
%%------------------------------------------------------------------------------

namespace() ->
    "gcp_pubsub".

fields(auth_service_account_json) ->
    [
        {type,
            mk(service_account_json, #{
                required => true, desc => ?DESC("auth_service_account_json")
            })},
        {service_account_json,
            mk(
                binary(),
                #{
                    required => false,
                    validator => fun ?MODULE:service_account_json_validator/1,
                    sensitive => true,
                    desc => ?DESC("service_account_json")
                }
            )}
    ].

desc(authentication) ->
    ?DESC(authentication);
desc(auth_service_account_json) ->
    ?DESC(auth_service_account_json);
desc(_) ->
    undefined.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
