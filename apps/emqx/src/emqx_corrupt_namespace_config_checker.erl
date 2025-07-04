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

-define(ALARM, <<"invalid_namespaced_configs">>).

-define(problems, problems).

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
            State = #{
                ?problems => #{}
            },
            {ok, State, {continue, #check_all{}}}
    end.

handle_continue(#check_all{}, State0) ->
    Problems =
        lists:foldl(
            fun({Ns, ErrorsPerRoot0}, Acc) ->
                Acc#{Ns => format_errors(ErrorsPerRoot0)}
            end,
            #{},
            emqx_config:get_all_namespace_config_errors()
        ),
    raise_alarms(Problems),
    ?tp("corrupt_ns_checker_started", #{}),
    State = State0#{?problems := Problems},
    {noreply, State}.

handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(#clear{ns = Namespace}, State0) ->
    #{?problems := Problems0} = State0,
    Problems = maps:remove(Namespace, Problems0),
    raise_alarms(Problems),
    ?tp("corrupt_ns_checker_checked", #{ns => Namespace}),
    State = State0#{?problems := Problems},
    {noreply, State};
handle_cast(#check{ns = Namespace}, State0) ->
    State = do_check(Namespace, State0),
    ?tp("corrupt_ns_checker_checked", #{ns => Namespace}),
    {noreply, State};
handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

raise_alarms(#{} = Problems) when map_size(Problems) == 0 ->
    emqx_alarm:ensure_deactivated(?ALARM);
raise_alarms(#{} = Problems) ->
    _ = emqx_alarm:safe_activate(
        ?ALARM,
        #{problems => Problems},
        <<"Namespaces with invalid configurations">>
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

do_check(Namespace, State0) ->
    #{?problems := Problems0} = State0,
    case emqx_config:get_namespace_config_errors(Namespace) of
        undefined ->
            Problems = maps:remove(Namespace, Problems0),
            raise_alarms(Problems);
        #{} = HoconErrorsPerRoot0 ->
            HoconErrorsPerRoot = format_errors(HoconErrorsPerRoot0),
            Problems = Problems0#{Namespace => HoconErrorsPerRoot},
            raise_alarms(Problems)
    end,
    State0#{?problems := Problems}.
