%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_authn_schema).

-elvis([{elvis_style, invalid_dynamic_call, disable}]).
-include_lib("hocon/include/hoconsc.hrl").

-export([
    common_fields/0,
    roots/0,
    tags/0,
    fields/1,
    authenticator_type/0,
    authenticator_type_without_scram/0,
    root_type/0,
    mechanism/1,
    backend/1
]).

roots() -> [].

tags() ->
    [<<"Authentication">>].

common_fields() ->
    [{enable, fun enable/1}].

enable(type) -> boolean();
enable(default) -> true;
enable(desc) -> ?DESC(?FUNCTION_NAME);
enable(_) -> undefined.

authenticator_type() ->
    hoconsc:union(union_member_selector(emqx_authn:providers())).

authenticator_type_without_scram() ->
    Providers = lists:filtermap(
        fun
            ({{scram, _Backend}, _Mod}) ->
                false;
            (_) ->
                true
        end,
        emqx_authn:providers()
    ),
    hoconsc:union(union_member_selector(Providers)).

config_refs(Providers) ->
    lists:append([Module:refs() || {_, Module} <- Providers]).

union_member_selector(Providers) ->
    Types = config_refs(Providers),
    fun
        (all_union_members) -> Types;
        ({value, Value}) -> select_union_member(Value, Providers)
    end.

select_union_member(#{<<"mechanism">> := _} = Value, Providers) ->
    select_union_member(Value, Providers, #{});
select_union_member(_Value, _) ->
    throw(#{hint => "missing 'mechanism' field"}).

select_union_member(Value, [], ReasonsMap) when ReasonsMap =:= #{} ->
    BackendVal = maps:get(<<"backend">>, Value, undefined),
    MechanismVal = maps:get(<<"mechanism">>, Value),
    throw(#{
        backend => BackendVal,
        mechanism => MechanismVal,
        hint => "unknown_mechanism_or_backend"
    });
select_union_member(_Value, [], ReasonsMap) ->
    throw(ReasonsMap);
select_union_member(Value, [Provider | Providers], ReasonsMap) ->
    {Mechanism, Backend, Module} =
        case Provider of
            {{M, B}, Mod} -> {atom_to_binary(M), atom_to_binary(B), Mod};
            {M, Mod} when is_atom(M) -> {atom_to_binary(M), undefined, Mod}
        end,
    case do_select_union_member(Mechanism, Backend, Module, Value) of
        {ok, Type} ->
            [Type];
        {error, nomatch} ->
            %% obvious mismatch, do not complain
            %% e.g. when 'backend' is "http", but the value is "redis",
            %% then there is no need to record the error like
            %% "'http' is exepcted but got 'redis'"
            select_union_member(Value, Providers, ReasonsMap);
        {error, Reason} ->
            %% more interesting error message
            %% e.g. when 'backend' is "http", but there is no "method" field
            %% found so there is no way to tell if it's the 'get' type or 'post' type.
            %% hence the error message is like:
            %% #{emqx_auth_http => "'http' auth backend must have get|post as 'method'"}
            select_union_member(Value, Providers, ReasonsMap#{Module => Reason})
    end.

do_select_union_member(Mechanism, Backend, Module, Value) ->
    BackendVal = maps:get(<<"backend">>, Value, undefined),
    MechanismVal = maps:get(<<"mechanism">>, Value),
    case MechanismVal =:= Mechanism of
        true when Backend =:= undefined ->
            case BackendVal =:= undefined of
                true ->
                    %% e.g. jwt has no 'backend'
                    try_select_union_member(Module, Value);
                false ->
                    {error, "unexpected 'backend' for " ++ binary_to_list(Mechanism)}
            end;
        true ->
            case Backend =:= BackendVal of
                true ->
                    try_select_union_member(Module, Value);
                false ->
                    %% 'backend' not matching
                    {error, nomatch}
            end;
        false ->
            %% 'mechanism' not matching
            {error, nomatch}
    end.

try_select_union_member(Module, Value) ->
    try
        %% some modules have refs/1 exported to help selectin the sub-types
        %% emqx_authn_http, emqx_authn_jwt, emqx_authn_mongodb and emqx_authn_redis
        Module:refs(Value)
    catch
        error:undef ->
            %% otherwise expect only one member from this module
            [Type] = Module:refs(),
            {ok, Type}
    end.

%% authn is a core functionality however implemented outside of emqx app
%% in emqx_schema, 'authentication' is a map() type which is to allow
%% EMQX more pluggable.
root_type() ->
    hoconsc:array(authenticator_type()).

mechanism(Name) ->
    ?HOCON(
        Name,
        #{
            required => true,
            desc => ?DESC("mechanism")
        }
    ).

backend(Name) ->
    ?HOCON(Name, #{
        required => true,
        desc => ?DESC("backend")
    }).

fields("metrics_status_fields") ->
    [
        {"resource_metrics", ?HOCON(?R_REF("resource_metrics"), #{desc => ?DESC("metrics")})},
        array("node_resource_metrics", "node_metrics"),
        {"metrics", ?HOCON(?R_REF("metrics"), #{desc => ?DESC("metrics")})},
        array("node_metrics"),
        {"status", ?HOCON(cluster_status(), #{desc => ?DESC("status")})},
        array("node_status"),
        array("node_error")
    ];
fields("metrics") ->
    [
        {"nomatch", ?HOCON(integer(), #{desc => ?DESC("metrics_nomatch")})},
        {"total", ?HOCON(integer(), #{desc => ?DESC("metrics_total")})},
        {"success", ?HOCON(integer(), #{desc => ?DESC("metrics_success")})},
        {"failed", ?HOCON(integer(), #{desc => ?DESC("metrics_failed")})},
        {"rate", ?HOCON(float(), #{desc => ?DESC("metrics_rate")})},
        {"rate_max", ?HOCON(float(), #{desc => ?DESC("metrics_rate_max")})},
        {"rate_last5m", ?HOCON(float(), #{desc => ?DESC("metrics_rate_last5m")})}
    ];
fields("resource_metrics") ->
    common_field();
fields("node_metrics") ->
    [
        node_name(),
        {"metrics", ?HOCON(?R_REF("metrics"), #{desc => ?DESC("metrics")})}
    ];
fields("node_resource_metrics") ->
    [
        node_name(),
        {"metrics", ?HOCON(?R_REF("resource_metrics"), #{desc => ?DESC("metrics")})}
    ];
fields("node_status") ->
    [
        node_name(),
        {"status", ?HOCON(status(), #{desc => ?DESC("node_status")})}
    ];
fields("node_error") ->
    [
        node_name(),
        {"error", ?HOCON(string(), #{desc => ?DESC("node_error")})}
    ].

common_field() ->
    [
        {"matched", ?HOCON(integer(), #{desc => ?DESC("matched")})},
        {"success", ?HOCON(integer(), #{desc => ?DESC("success")})},
        {"failed", ?HOCON(integer(), #{desc => ?DESC("failed")})},
        {"rate", ?HOCON(float(), #{desc => ?DESC("rate")})},
        {"rate_max", ?HOCON(float(), #{desc => ?DESC("rate_max")})},
        {"rate_last5m", ?HOCON(float(), #{desc => ?DESC("rate_last5m")})}
    ].

status() ->
    hoconsc:enum([connected, disconnected, connecting]).

cluster_status() ->
    hoconsc:enum([connected, disconnected, connecting, inconsistent]).

node_name() ->
    {"node", ?HOCON(binary(), #{desc => ?DESC("node"), example => "emqx@127.0.0.1"})}.

array(Name) ->
    array(Name, Name).

array(Name, DescId) ->
    {Name, ?HOCON(?R_REF(Name), #{desc => ?DESC(DescId)})}.
