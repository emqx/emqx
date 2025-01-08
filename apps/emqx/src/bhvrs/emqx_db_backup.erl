%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_db_backup).

-export([backup_tables/1]).

-type traverse_break_reason() :: over | migrate.

-type table_set_name() :: binary().

-type opts() :: #{print_fun => fun((io:format(), [term()]) -> ok)}.

-callback backup_tables() -> {table_set_name(), [mria:table()]}.

%% validate the backup
%% return `ok` to traverse the next item
%% return `{ok, over}` to finish the traverse
%% return `{ok, migrate}` to call the migration callback
-callback validate_mnesia_backup(tuple()) ->
    ok
    | {ok, traverse_break_reason()}
    | {error, term()}.

-callback migrate_mnesia_backup(tuple()) -> {ok, tuple()} | {error, term()}.

%% NOTE: currently, this is called only when the table has been restored successfully.
-callback on_backup_table_imported(mria:table(), opts()) -> ok | {error, term()}.

-optional_callbacks([validate_mnesia_backup/1, migrate_mnesia_backup/1, on_backup_table_imported/2]).

-export_type([traverse_break_reason/0]).

-spec backup_tables(module()) -> {table_set_name(), [mria:table()]}.
backup_tables(Mod) ->
    Mod:backup_tables().
