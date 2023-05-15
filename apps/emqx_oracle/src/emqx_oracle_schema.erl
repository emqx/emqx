%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_oracle_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-define(REF_MODULE, emqx_oracle).

%% Hocon config schema exports
-export([
    roots/0,
    fields/1
]).

roots() ->
    [{config, #{type => hoconsc:ref(?REF_MODULE, config)}}].

fields(config) ->
    [{server, server()}, {sid, fun sid/1}] ++
        emqx_connector_schema_lib:relational_db_fields() ++
        emqx_connector_schema_lib:prepare_statement_fields().

server() ->
    Meta = #{desc => ?DESC(?REF_MODULE, "server")},
    emqx_schema:servers_sc(Meta, (?REF_MODULE):oracle_host_options()).

sid(type) -> binary();
sid(desc) -> ?DESC(?REF_MODULE, "sid");
sid(required) -> true;
sid(_) -> undefined.
