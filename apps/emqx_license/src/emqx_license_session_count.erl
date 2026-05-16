%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_session_count).

-moduledoc """
Registry of session-count contributors.

Other apps (e.g. `emqx_gateway`) register a 0-arity callback that returns the
number of sessions they account for. The license resources collector calls
`sum_callbacks/0` on each tick and adds the total to the broker session count.

Storage is the `emqx_license` application env so the registry survives without
an extra process. Callbacks whose module is not loaded (feature off) raise
`error:undef` and are skipped silently; any other crash is logged and the
broken callback's contribution is dropped for that tick.
""".

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    register_callback/2,
    unregister_callback/1,
    sum_callbacks/0
]).

-define(APP, emqx_license).
-define(ENV_KEY, session_count_callbacks).

-type name() :: atom().
-type callback() :: fun(() -> non_neg_integer()).

-doc "Register `Fun` under `Name`. Overwrites any previous binding for `Name`.".
-spec register_callback(name(), callback()) -> ok.
register_callback(Name, Fun) when is_atom(Name), is_function(Fun, 0) ->
    Map = get_callbacks(),
    application:set_env(?APP, ?ENV_KEY, Map#{Name => Fun}).

-doc "Remove the callback registered under `Name`. No-op if absent.".
-spec unregister_callback(name()) -> ok.
unregister_callback(Name) when is_atom(Name) ->
    Map = get_callbacks(),
    application:set_env(?APP, ?ENV_KEY, maps:remove(Name, Map)).

-doc "Sum the integers returned by every registered callback. See module doc for failure handling.".
-spec sum_callbacks() -> non_neg_integer().
sum_callbacks() ->
    maps:fold(fun safe_add/3, 0, get_callbacks()).

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

get_callbacks() ->
    application:get_env(?APP, ?ENV_KEY, #{}).

safe_add(Name, Fun, Acc) ->
    try Fun() of
        N when is_integer(N), N >= 0 ->
            Acc + N;
        Other ->
            Event = #{
                msg => license_session_count_callback_bad_return,
                callback => Name,
                return => Other
            },
            ?SLOG(error, Event),
            ?tp(license_session_count_callback_bad_return, Event),
            Acc
    catch
        error:undef ->
            Acc;
        Class:Reason:Stack ->
            Event = #{
                msg => license_session_count_callback_crash,
                callback => Name,
                class => Class,
                reason => Reason,
                stacktrace => Stack
            },
            ?SLOG(error, Event),
            ?tp(license_session_count_callback_crash, Event),
            Acc
    end.
