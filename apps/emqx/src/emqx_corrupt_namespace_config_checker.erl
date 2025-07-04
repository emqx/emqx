%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_corrupt_namespace_config_checker).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%% API
-export([
    start_link/0,

    clear/1,
    check/1
]).

%% `gen_server' API
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%% Calls/casts/infos
-record(check_all, {}).
-record(clear, {ns}).
-record(check, {ns}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

clear(Namespace) ->
    gen_server:cast(?MODULE, #clear{ns = Namespace}).

check(Namespace) ->
    gen_server:cast(?MODULE, #check{ns = Namespace}).

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(_) ->
    case mria_config:whoami() of
        replicant ->
            ignore;
        _ ->
            State = #{},
            {ok, State, {continue, #check_all{}}}
    end.

handle_continue(#check_all{}, State) ->
    NamespaceErrors = emqx_config:get_all_namespace_config_errors(),
    raise_alarms(NamespaceErrors),
    ?tp("corrupt_ns_checker_started", #{}),
    {noreply, State}.

handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(#clear{ns = Namespace}, State) ->
    emqx_alarm:ensure_deactivated(Namespace),
    ?tp("corrupt_ns_checker_cleared", #{ns => Namespace}),
    {noreply, State};
handle_cast(#check{ns = Namespace}, State) ->
    do_check(Namespace),
    ?tp("corrupt_ns_checker_checked", #{ns => Namespace}),
    {noreply, State};
handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

raise_alarms(NamespaceErrors) when is_list(NamespaceErrors) ->
    lists:foreach(fun raise_alarms/1, NamespaceErrors);
raise_alarms({Namespace, HoconErrorsPerRoot0}) ->
    HoconErrorsPerRoot = format_errors(HoconErrorsPerRoot0),
    emqx_alarm:safe_activate(
        Namespace,
        #{namespace => Namespace, errors => HoconErrorsPerRoot},
        <<"Namespace contains invalid configuration">>
    ),
    ok.

format_errors(HoconErrorsPerRoot0) ->
    SchemaMod = emqx_config:get_schema_mod(),
    maps:map(
        fun(_Root, HoconErrors) ->
            case emqx_hocon:format_error({SchemaMod, HoconErrors}) of
                false ->
                    %% Should be impossible
                    HoconErrors;
                {ok, Formatted} ->
                    emqx_utils_json:decode(Formatted)
            end
        end,
        HoconErrorsPerRoot0
    ).

do_check(Namespace) ->
    case emqx_config:get_namespace_config_errors(Namespace) of
        undefined ->
            emqx_alarm:ensure_deactivated(Namespace),
            ok;
        #{} = HoconErrorsPerRoot0 ->
            HoconErrorsPerRoot = format_errors(HoconErrorsPerRoot0),
            emqx_alarm:safe_activate(
                Namespace,
                #{namespace => Namespace, errors => HoconErrorsPerRoot},
                <<"Namespace contains invalid configuration">>
            ),
            ok
    end.
