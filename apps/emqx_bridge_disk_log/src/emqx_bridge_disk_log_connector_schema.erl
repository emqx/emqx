%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_disk_log_connector_schema).

-behaviour(hocon_schema).
-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_bridge_disk_log.hrl").

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

%% `emqx_schema_hooks' API
-export([injected_fields/0]).

%% API
-export([
    unique_filepath_validator/1
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% `hocon_schema' API
%%------------------------------------------------------------------------------

namespace() ->
    "connector_disk_log".

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
        {filepath, mk(binary(), #{required => true, desc => ?DESC("filepath")})},
        {max_file_size,
            mk(emqx_schema:bytesize(), #{required => true, desc => ?DESC("max_file_size")})},
        {max_file_number, mk(pos_integer(), #{required => true, desc => ?DESC("max_file_number")})}
    ] ++
        emqx_connector_schema:resource_opts().

desc("config_connector") ->
    ?DESC("config_connector");
desc(resource_opts) ->
    ?DESC(emqx_resource_schema, resource_opts);
desc(_Name) ->
    undefined.

%%------------------------------------------------------------------------------
%% `emqx_connector_examples' API
%%------------------------------------------------------------------------------

connector_examples(Method) ->
    [
        #{
            <<"disk_log">> => #{
                summary => <<"Disk Log Connector">>,
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
        filepath => <<"/tmp/my_log">>,
        max_file_size => <<"10MB">>,
        max_file_number => 7,
        resource_opts => #{
            health_check_interval => <<"45s">>,
            start_after_created => true,
            start_timeout => <<"5s">>
        }
    }.

%%------------------------------------------------------------------------------
%% `emqx_schema_hooks' API
%%------------------------------------------------------------------------------

injected_fields() ->
    #{
        'connectors.validators' => [fun ?MODULE:unique_filepath_validator/1]
    }.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

unique_filepath_validator(#{?CONNECTOR_TYPE_BIN := DiskLogConnectors}) when
    map_size(DiskLogConnectors) > 0
->
    FilepathsToConnectors =
        maps:groups_from_list(
            fun({_Name, #{<<"filepath">> := Filepath}}) -> Filepath end,
            fun({Name, _Config}) -> Name end,
            maps:to_list(DiskLogConnectors)
        ),
    Duplicated0 = maps:filter(fun(_, Vs) -> length(Vs) > 1 end, FilepathsToConnectors),
    Duplicated = maps:values(Duplicated0),
    case Duplicated of
        [] ->
            ok;
        [_ | _] ->
            DuplicatedFormatted = format_duplicated_name_groups(Duplicated),
            Msg =
                iolist_to_binary(
                    io_lib:format(
                        "disk_log connectors must not use the same filepath;"
                        " connectors with duplicate filepaths: ~s",
                        [DuplicatedFormatted]
                    )
                ),
            {error, Msg}
    end;
unique_filepath_validator(_X) ->
    ok.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).

format_duplicated_name_groups(DuplicatedNameGroups) ->
    lists:join(
        $;,
        lists:map(
            fun(NameGroup) ->
                lists:join($,, lists:sort(NameGroup))
            end,
            DuplicatedNameGroups
        )
    ).
