%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_telemetry_schema).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%% 'emqxtel' to distinguish open-telemetry
namespace() -> "emqxtel".

roots() -> ["telemetry"].

fields("telemetry") ->
    %% the 'enable' field has no default value because it is resovled
    %% dynamically if it is not explicitly set
    [{enable, ?HOCON(boolean(), #{required => false, desc => ?DESC("enable")})}].

desc("telemetry") ->
    ?DESC("telemetry_root_doc");
desc(_) ->
    undefined.
