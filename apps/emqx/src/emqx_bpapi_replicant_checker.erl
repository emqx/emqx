%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bpapi_replicant_checker).

%% @doc This process monitors whether the BPAPI entries for the current replicant node are
%% lost due to connecting to a new core node which has no memory of the replicant, and
%% re-announces its BPAPIs if necessary.
%% This process is only started on replicants.

-feature(maybe_expr, enable).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-include("./bpapi/emqx_bpapi.hrl").

%% API
-export([
    start_link/0
]).

%% `gen_server' API
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link(?MODULE, [], []).

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(_) ->
    case mria_config:whoami() of
        replicant ->
            {ok, _} = mnesia:subscribe({table, ?TAB, simple}),
            State = #{},
            {ok, State};
        _ ->
            ignore
    end.

handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info({mnesia_table_event, {delete, {schema, ?TAB}, _TId}}, State) ->
    %% Replicant is probably connecting to a core and will bootstrap this table soon.
    {noreply, State};
handle_info({mnesia_table_event, {write, {schema, ?TAB, _Def}, _TId}}, State) ->
    %% Replicant has connected to a core and is possibly bootstrap this table.
    %% We can wait for it to be ready and then reannounce our BPAPI, if needed.
    maybe_reannounce_bpapis(),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

maybe_reannounce_bpapis() ->
    ok = mria:wait_for_tables([?TAB]),
    case emqx_bpapi:supported_apis(node()) of
        [_ | _] ->
            %% There are entries; we assume they are correct.
            ok;
        [] ->
            %% Probably joined a core node that has no recollection of us; reannounce.
            emqx_bpapi:announce(node(), emqx),
            ?tp(warning, "bpapi_replicant_checker_reannounced", #{})
    end.
