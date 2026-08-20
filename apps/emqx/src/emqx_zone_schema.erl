%%--------------------------------------------------------------------
%% Copyright (c) 2021-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_zone_schema).
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([namespace/0, roots/0, fields/1, desc/1]).
-export([zones_without_default/0, global_zone_with_default/0]).

namespace() -> zone.

%% Zone values are never checked as root level.
%% We need roots defined here because it's used to generate config API schema.
roots() ->
    [
        mqtt,
        stats,
        flapping_detect,
        force_shutdown,
        conn_congestion,
        force_gc,
        overload_protection,
        durable_sessions
    ].

zones_without_default() ->
    Fields = roots(),
    Hidden = hidden(),
    lists:map(
        fun(F) ->
            case lists:member(F, Hidden) of
                true ->
                    {F,
                        ?HOCON(
                            ?R_REF(?MODULE, atom_to_list(F)),
                            (root_meta(F))#{importance => ?IMPORTANCE_HIDDEN}
                        )};
                false ->
                    {F, ?HOCON(?R_REF(?MODULE, atom_to_list(F)), root_meta(F))}
            end
        end,
        Fields
    ).

global_zone_with_default() ->
    lists:map(
        fun(F) -> {F, ?HOCON(?R_REF(emqx_schema, atom_to_list(F)), root_meta(F))} end,
        roots() -- hidden()
    ).

%% The flapping_detect converter must apply wherever the struct is
%% referenced, so that deprecated flat fields are lifted into
%% `by_clientid` in zone and global-zone configs too.
root_meta(flapping_detect) ->
    #{converter => fun emqx_schema:flapping_detect_converter/2};
root_meta(_) ->
    #{}.

hidden() ->
    [
        stats,
        overload_protection,
        conn_congestion
    ].

%% zone schemas are clones from the same name from root level
%% only not allowed to have default values.
fields("flapping_detect") ->
    lists:map(
        fun
            ({N, Sc}) when N =:= "by_clientid"; N =:= "by_username"; N =:= "by_peerhost" ->
                %% Redirect the dimension struct to this module so its
                %% defaults are stripped too: dimension fields left unset
                %% in a zone override then inherit the global dimension
                %% config through the zone merge.
                {N, no_default_dimension(Sc)};
            ({N, Sc}) ->
                {N, no_default(Sc)}
        end,
        emqx_schema:fields("flapping_detect")
    );
fields(Name) ->
    [{N, no_default(Sc)} || {N, Sc} <- emqx_schema:fields(Name)].

desc(Name) ->
    emqx_schema:desc(Name).

%% no default values for zone settings, don't required either.
no_default(Sc) ->
    fun
        (default) -> undefined;
        (required) -> false;
        (Other) -> hocon_schema:field_schema(Sc, Other)
    end.

no_default_dimension(Sc) ->
    fun
        (type) -> ?UNION([none, ?R_REF(?MODULE, "flapping_detect_dimension")]);
        (default) -> undefined;
        (required) -> false;
        (Other) -> hocon_schema:field_schema(Sc, Other)
    end.
