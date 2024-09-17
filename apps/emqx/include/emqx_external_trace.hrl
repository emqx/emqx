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

-define(ATTRS_META, attrs_meta).

-define(EXT_TRACE_ATTRS_META(Meta),
    {emqx_ext_trace, ?ATTRS_META, Meta}
).

-define(ext_trace_add_attrs(META),
    emqx_external_trace:add_span_attrs(META)
).

-define(ext_trace_add_event(EVENT_NAME, TRACE_ATTRS),
    emqx_external_trace:add_span_event(
        EVENT_NAME,
        TRACE_ATTRS
    )
).

%% --------------------------------------------------------------------
%% types

-type attrs() :: #{atom() => _}.

-type attrs_meta() :: {emqx_ext_trace, ?ATTRS_META, any()}.

-type event_name() :: opentelemetry:event_name().

-endif.
