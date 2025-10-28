%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_retainer_dummy).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(emqx_retainer).
-behaviour(emqx_retainer_gc).

-export([
    create/1,
    update/2,
    close/1,
    delete_message/2,
    store_retained/2,
    read_message/2,
    page_read/5,
    match_messages/3,
    delete_cursor/2,
    clear_expired/3,
    clean/1,
    size/1,
    current_index_incarnation/1,
    cursor_index_incarnation/2
]).

-behaviour(emqx_schema_hooks).

-export([
    fields/1,
    injected_fields/0
]).

injected_fields() ->
    #{
        'retainer.external_backends' => external_backend_fields()
    }.

create(_Config) -> #{}.

update(_Context, _Config) -> ok.

close(_Context) -> ok.

delete_message(_Context, _Topic) -> ok.

store_retained(_Context, _Message) -> ok.

read_message(_Context, _Topic) -> {ok, []}.

page_read(_Context, _Topic, _Deadline, _Offset, _Limit) -> {ok, false, []}.

match_messages(_Context, _Topic, _Cursor) -> {ok, [], 0}.

delete_cursor(_Context, _Cursor) -> ok.

clear_expired(_Context, _Deadline, _Limit) -> {true, 0}.

clean(_Context) -> ok.

size(_Context) -> 0.

current_index_incarnation(_Context) -> 0.

cursor_index_incarnation(_Context, _Cursor) -> 0.

external_backend_fields() ->
    [
        {dummy, hoconsc:ref(?MODULE, dummy)}
    ].

fields(dummy) ->
    [
        {module,
            hoconsc:mk(
                emqx_retainer_dummy,
                #{
                    desc => <<"dummy backend mod">>,
                    required => false,
                    default => <<"emqx_retainer_dummy">>,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {enable,
            hoconsc:mk(
                boolean(),
                #{
                    desc => <<"enable dummy backend">>,
                    required => true,
                    default => false
                }
            )}
    ].
