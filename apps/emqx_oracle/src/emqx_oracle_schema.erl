%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_oracle_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-define(REF_MODULE, emqx_oracle).

%% Hocon config schema exports
-export([
    roots/0,
    fields/1,
    namespace/0
]).

namespace() -> oracle.

roots() ->
    [].

fields(config) ->
    Fields =
        [{server, server()}, {sid, fun sid/1}, {service_name, fun service_name/1}] ++
            adjust_fields(emqx_connector_schema_lib:relational_db_fields()) ++
            emqx_connector_schema_lib:prepare_statement_fields(),
    proplists:delete(database, Fields).

server() ->
    Meta = #{desc => ?DESC(?REF_MODULE, "server")},
    emqx_schema:servers_sc(Meta, (?REF_MODULE):oracle_host_options()).

sid(type) -> binary();
sid(desc) -> ?DESC(?REF_MODULE, "sid");
sid(required) -> false;
sid(_) -> undefined.

service_name(type) -> binary();
service_name(desc) -> ?DESC(?REF_MODULE, "service_name");
service_name(required) -> false;
service_name(_) -> undefined.

adjust_fields(Fields) ->
    lists:map(
        fun
            ({username, Sc}) ->
                %% to please dialyzer...
                Override = #{type => hocon_schema:field_schema(Sc, type), required => true},
                {username, hocon_schema:override(Sc, Override)};
            (Field) ->
                Field
        end,
        Fields
    ).
