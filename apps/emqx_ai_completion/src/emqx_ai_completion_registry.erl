%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_registry).

-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx/include/logger.hrl").

-export([
    create_tab/0
]).

-export([
    create/3,
    destroy/1,
    update/2,
    call/3,
    call/2
]).

-define(TAB, ?MODULE).

-type completion_name() :: emqx_ai_completion:completion_name().
-type options() :: emqx_ai_completion:options().
-type state() :: emqx_ai_completion:state().

-record(completion, {
    name :: completion_name(),
    module :: module(),
    options :: options(),
    state :: state()
}).

create_tab() ->
    emqx_utils_ets:new(?TAB, [
        set,
        named_table,
        public,
        {read_concurrency, true},
        {keypos, #completion.name}
    ]).

create(Name, Module, Options) ->
    try
        State = Module:create(Options),
        _ = ets:insert(?TAB, #completion{name = Name, module = Module, options = Options, state = State}),
        ok
    catch
        _:Reason ->
            ?tp(error, failed_to_create_completion, #{
                name => Name,
                reason => Reason
            }),
            error(Reason)
    end.

destroy(Name) ->
    case lookup(Name) of
        {ok, #completion{module = Module, state = State}} ->
            try
                _ = Module:destroy(State)
            catch
                _:Reason ->
                    ?tp(error, failed_to_destroy_completion, #{
                        name => Name,
                        reason => Reason
                    })
            end,
            ets:delete(?TAB, Name);
        not_found ->
            error(completion_not_found)
    end.

update(Name, Options) ->
    case lookup(Name) of
        {ok, #completion{module = Module, state = State}} ->
            try
                _ = Module:update_options(State, Options)
            catch
                _:Reason ->
                    ?tp(error, failed_to_update_completion, #{
                        name => Name,
                        reason => Reason
                    })
            end;
        not_found ->
            error(completion_not_found)
    end.


call(Name, Prompt, Data) ->
    case lookup(Name) of
        {ok, #completion{module = Module, state = State}} ->
            Module:call_completion(State, Prompt, Data);
        not_found ->
            error(completion_not_found)
    end.

call(Name, Data) ->
    case lookup(Name) of
        {ok, #completion{module = Module, state = State, options = Options}} ->
            Prompt = maps:get(prompt, Options, <<"">>),
            Module:call_completion(State, Prompt, Data);
        not_found ->
            error(completion_not_found)
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

lookup(Name) ->
    case ets:lookup(?TAB, Name) of
        [#completion{} = Completion] ->
            {ok, Completion};
        [] ->
            not_found
    end.
