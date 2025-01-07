%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_s3).

-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_bridge_s3.hrl").

-behaviour(hocon_schema).
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-export([
    connector_examples/1
]).

-export([
    pre_config_update/4
]).

%%-------------------------------------------------------------------------------------------------
%% `hocon_schema' API
%%-------------------------------------------------------------------------------------------------

namespace() ->
    "bridge_s3".

roots() ->
    [].

fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    emqx_connector_schema:api_fields(Field, ?CONNECTOR, fields(s3_connector_config));
fields("config_connector") ->
    emqx_connector_schema:common_fields() ++ fields(s3_connector_config);
fields(s3_connector_config) ->
    emqx_s3_schema:fields(s3_client) ++
        emqx_connector_schema:resource_opts_ref(?MODULE, s3_connector_resource_opts);
fields(s3_connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields().

desc("config_connector") ->
    ?DESC(config_connector);
desc(s3_connector_resource_opts) ->
    ?DESC(emqx_resource_schema, resource_opts);
desc(_Name) ->
    undefined.

%% Examples

connector_examples(Method) ->
    [
        #{
            <<"s3_aws">> => #{
                summary => <<"S3 Connector">>,
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
            type => atom_to_binary(?CONNECTOR),
            name => <<"my_s3_connector">>
        }
    );
connector_example(put) ->
    #{
        enable => true,
        description => <<"My S3 connector">>,
        host => <<"s3.eu-east-1.amazonaws.com">>,
        port => 443,
        access_key_id => <<"ACCESS">>,
        secret_access_key => <<"SECRET">>,
        transport_options => #{
            ssl => #{
                enable => true,
                verify => <<"verify_peer">>
            },
            connect_timeout => <<"1s">>,
            request_timeout => <<"60s">>,
            pool_size => 4,
            max_retries => 1,
            enable_pipelining => 1
        }
    }.

%% Config update

pre_config_update(Path, _Name, Conf = #{<<"transport_options">> := TransportOpts}, _ConfOld) ->
    case emqx_connector_ssl:convert_certs(filename:join(Path), TransportOpts) of
        {ok, NTransportOpts} ->
            {ok, Conf#{<<"transport_options">> := NTransportOpts}};
        {error, Error} ->
            {error, Error}
    end;
pre_config_update(_Path, _Name, Conf, _ConfOld) ->
    {ok, Conf}.
