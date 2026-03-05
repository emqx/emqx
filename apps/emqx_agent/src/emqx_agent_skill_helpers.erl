%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_skill_helpers).

-export([correlation/2]).

%% Copies correlation fields from an invoke Request into a reply skeleton.
%%
%% Fields forwarded: req_id (required), trace_id, iid, sid (optional, default null).
%% The caller merges the returned map with skill-specific fields.
-spec correlation(Request :: map(), Extra :: map()) -> map().
correlation(Request, Extra) ->
    maps:merge(
        #{
            <<"req_id">> => maps:get(<<"req_id">>, Request),
            <<"trace_id">> => maps:get(<<"trace_id">>, Request, null),
            <<"iid">> => maps:get(<<"iid">>, Request, null),
            <<"sid">> => maps:get(<<"sid">>, Request, null)
        },
        Extra
    ).
