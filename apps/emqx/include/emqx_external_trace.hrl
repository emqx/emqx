%%--------------------------------------------------------------------
%% Copyright (c) 2019-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_EXT_TRACE_HRL).
-define(EMQX_EXT_TRACE_HRL, true).

%% --------------------------------------------------------------------
%% Macros

-define(EXT_TRACE_START, '$ext_trace_start').
-define(EXT_TRACE_STOP, '$ext_trace_stop').

-define(EMQX_EXTERNAL_MODULE, emqx_external_trace).
-define(PROVIDER, {?EMQX_EXTERNAL_MODULE, trace_provider}).

-ifndef(EMQX_RELEASE_EDITION).
-define(EMQX_RELEASE_EDITION, ce).
-endif.

-if(?EMQX_RELEASE_EDITION == ee).

-define(with_provider(IfRegistered, IfNotRegistered),
    fun() ->
        case persistent_term:get(?PROVIDER, undefined) of
            undefined ->
                IfNotRegistered;
            Provider ->
                Provider:IfRegistered
        end
    end()
).

-define(EXT_TRACE_ANY(FuncName, Any, Attrs),
    ?with_provider(
        FuncName(Any, Attrs),
        Any
    )
).

-define(EXT_TRACE_ADD_ATTRS(Attrs),
    ?with_provider(add_span_attrs(Attrs), ok)
).

-define(EXT_TRACE_ADD_ATTRS(Attrs, Ctx),
    ?with_provider(add_span_attrs(Attrs, Ctx), ok)
).

-define(EXT_TRACE_SET_STATUS_OK(),
    ?with_provider(
        set_status_ok(),
        ok
    )
).

-define(EXT_TRACE_SET_STATUS_ERROR(),
    ?with_provider(
        set_status_error(),
        ok
    )
).

-define(EXT_TRACE_SET_STATUS_ERROR(Msg),
    ?with_provider(
        set_status_error(Msg),
        ok
    )
).

-define(EXT_TRACE_WITH_ACTION_START(FuncName, Any, Attrs),
    ?with_provider(
        FuncName(?EXT_TRACE_START, Any, Attrs),
        Any
    )
).

-define(EXT_TRACE_WITH_ACTION_STOP(FuncName, Any, Attrs),
    ?with_provider(
        FuncName(?EXT_TRACE_STOP, Any, Attrs),
        ok
    )
).

-define(EXT_TRACE_WITH_PROCESS_FUN(FuncName, Any, Attrs, ProcessFun),
    ?with_provider(
        FuncName(Any, Attrs, ProcessFun),
        ProcessFun(Any)
    )
).

-type event_name() :: opentelemetry:event_name().

-else.

-define(EXT_TRACE_ANY(_FuncName, Any, _Attrs), Any).
-define(EXT_TRACE_ADD_ATTRS(_Attrs), ok).
-define(EXT_TRACE_ADD_ATTRS(_Attrs, _Ctx), ok).
-define(EXT_TRACE_SET_STATUS_OK(), ok).
-define(EXT_TRACE_SET_STATUS_ERROR(), ok).
-define(EXT_TRACE_SET_STATUS_ERROR(_), ok).
-define(EXT_TRACE_WITH_ACTION_START(_FuncName, Any, _Attrs), Any).
-define(EXT_TRACE_WITH_ACTION_STOP(_FuncName, Any, _Attrs), ok).
-define(EXT_TRACE_WITH_PROCESS_FUN(_FuncName, Any, _Attrs, ProcessFun), ProcessFun(Any)).

-endif.

%% --------------------------------------------------------------------
%% types

-type attrs() :: #{atom() => _}.

-endif.
