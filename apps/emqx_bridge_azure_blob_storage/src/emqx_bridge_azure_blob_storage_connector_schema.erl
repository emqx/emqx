%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_azure_blob_storage_connector_schema).

-behaviour(hocon_schema).
-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% `hocon_schema' API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%% `emqx_connector_examples' API
-export([
    connector_examples/1
]).

%% Internal exports
-export([validate_account_key/1]).

%% API
-export([]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(CONNECTOR_TYPE, azure_blob_storage).

%%-------------------------------------------------------------------------------------------------
%% `hocon_schema' API
%%-------------------------------------------------------------------------------------------------

namespace() ->
    "connector_azure_blob_storage".

roots() ->
    [].

fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    emqx_connector_schema:api_fields(Field, ?CONNECTOR_TYPE, fields(connector_config));
fields("config_connector") ->
    emqx_connector_schema:common_fields() ++ fields(connector_config);
fields(connector_config) ->
    [
        {account_name,
            mk(string(), #{
                required => true,
                desc => ?DESC("account_name")
            })},
        {account_key,
            emqx_schema_secret:mk(
                #{
                    required => true,
                    validator => fun account_key_validator/1,
                    desc => ?DESC("account_key")
                }
            )},
        {endpoint,
            mk(
                string(),
                #{
                    required => false,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )}
    ] ++
        emqx_connector_schema:resource_opts_ref(?MODULE, resource_opts);
fields(resource_opts) ->
    emqx_connector_schema:resource_opts_fields().

desc("config_connector") ->
    ?DESC("config_connector");
desc(resource_opts) ->
    ?DESC(emqx_resource_schema, resource_opts);
desc(_Name) ->
    undefined.

%%-------------------------------------------------------------------------------------------------
%% `emqx_connector_examples' API
%%-------------------------------------------------------------------------------------------------

connector_examples(Method) ->
    [
        #{
            <<"abs">> => #{
                summary => <<"Azure Blob Storage Connector">>,
                value => connector_example(Method)
            }
        }
    ].

connector_example(get) ->
    maps:merge(
        connector_example(put),
        #{
            status => <<"connected">>,
            node_status => [
                #{
                    node => <<"emqx@localhost">>,
                    status => <<"connected">>
                }
            ]
        }
    );
connector_example(post) ->
    maps:merge(
        connector_example(put),
        #{
            type => atom_to_binary(?CONNECTOR_TYPE),
            name => <<"my_connector">>
        }
    );
connector_example(put) ->
    #{
        enable => true,
        description => <<"My connector">>,
        account_name => <<"my_account_name">>,
        account_key => <<"******">>,
        resource_opts => #{
            health_check_interval => <<"45s">>,
            start_after_created => true,
            start_timeout => <<"5s">>
        }
    }.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Internal exports
%%------------------------------------------------------------------------------

validate_account_key(Val) ->
    try
        _ = base64:decode(emqx_secret:unwrap(Val)),
        ok
    catch
        _:_ ->
            {error, <<"bad account key; must be a valid base64 encoded value">>}
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).

account_key_validator(Val) ->
    case emqx_secret:unwrap(Val) of
        <<"******">> ->
            %% The frontend sends obfuscated values when updating a connector...  So we
            %% cannot distinguish an obfuscated value from an user explicitly setting this
            %% field to this value.
            ok;
        Key ->
            validate_account_key(Key)
    end.
