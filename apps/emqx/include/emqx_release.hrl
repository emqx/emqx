%%--------------------------------------------------------------------
%% Copyright (c) 2021-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% NOTE: this is the release version which is not always the same
%% as the emqx app version defined in emqx.app.src
%% App (plugin) versions are bumped independently.
%% e.g. EMQX_RELEASE_EE being 5.9.0 does not always imply emqx app
%% should be 5.9.0, as it might be the case that only one of the
%% plugins had a bug to fix. So for a hot beam upgrade, only the app
%% with beam files changed needs an upgrade.

%% NOTE: This version number should be manually bumped for each release

%% NOTE: This version number should have 3 numeric parts
%% (Major.Minor.Patch), and extra info can be added after a final
%% hyphen.

%% NOTE: Also make sure to follow the instructions in end of
%% `apps/emqx_bpapi/README.md'

-define(EMQX_RELEASE_EE, "6.3.3-rc.1").

%% Version of what a replicant node shares with its core nodes: the
%% mria table definitions, and the transaction BPAPI versions
%% (`<api>_tx_proto_v<N>' modules) whose funs a replicant passes to a
%% core by name. Two nodes with the same value here, and the same mria
%% application version, can serve each other as core and replicant.
%%
%% Bump it when either changes. CI compares both against the baseline
%% in apps/emqx_machine/priv/db_schema/<version>/ and fails until the
%% version is bumped and the baseline regenerated:
%%
%%   ./scripts/check-db-schema.escript --update
%%   ./scripts/test/db-schema-baseline-test.sh --update
-define(EMQX_DB_SCHEMA_VSN, "1").
