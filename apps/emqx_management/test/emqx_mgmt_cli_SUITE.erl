%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_cli_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    mria:start(),
    ok = emqx_common_test_helpers:start_apps([emqx_management]),
    emqx_common_test_helpers:start_apps([] ++ [emqx_dashboard], fun set_special_configs/1),
    Config.

end_per_suite(_) ->
    emqx_common_test_helpers:stop_apps([emqx_management] ++ [emqx_dashboard]),
    emqx_config:delete_override_conf_files(),
    ok.

set_special_configs(emqx_dashboard) ->
    emqx_dashboard_api_test_helpers:set_default_config();
set_special_configs(_App) ->
    ok.

t_status(_Config) ->
    emqx_ctl:run_command([]),
    emqx_ctl:run_command(["status"]),
    ok.

t_broker(_Config) ->
    %% broker         # Show broker version, uptime and description
    emqx_ctl:run_command(["broker"]),
    %% broker stats   # Show broker statistics of clients, topics, subscribers
    emqx_ctl:run_command(["broker", "stats"]),
    %% broker metrics # Show broker metrics
    emqx_ctl:run_command(["broker", "metrics"]),
    ok.

t_cluster(_Config) ->
    %% cluster join <Node>        # Join the cluster
    %% cluster leave              # Leave the cluster
    %% cluster force-leave <Node> # Force the node leave from cluster
    %% cluster status             # Cluster status
    emqx_ctl:run_command(["cluster", "status"]),
    ok.

t_clients(_Config) ->
    %% clients list            # List all clients
    emqx_ctl:run_command(["clients", "list"]),
    %% clients show <ClientId> # Show a client
    %% clients kick <ClientId> # Kick out a client
    ok.

t_routes(_Config) ->
    %% routes list         # List all routes
    emqx_ctl:run_command(["routes", "list"]),
    %% routes show <Topic> # Show a route
    ok.

t_subscriptions(_Config) ->
    %% subscriptions list                         # List all subscriptions
    emqx_ctl:run_command(["subscriptions", "list"]),
    %% subscriptions show <ClientId>              # Show subscriptions of a client
    %% subscriptions add <ClientId> <Topic> <QoS> # Add a static subscription manually
    %% subscriptions del <ClientId> <Topic>       # Delete a static subscription manually
    ok.

t_plugins(_Config) ->
    %% plugins <command> [Name-Vsn]          # e.g. 'start emqx_plugin_template-5.0-rc.1'
    %% plugins list                          # List all installed plugins
    emqx_ctl:run_command(["plugins", "list"]),
    %% plugins describe  Name-Vsn            # Describe an installed plugins
    %% plugins install   Name-Vsn            # Install a plugin package placed
    %%                                       # in plugin'sinstall_dir
    %% plugins uninstall Name-Vsn            # Uninstall a plugin. NOTE: it deletes
    %%                                       # all files in install_dir/Name-Vsn
    %% plugins start     Name-Vsn            # Start a plugin
    %% plugins stop      Name-Vsn            # Stop a plugin
    %% plugins restart   Name-Vsn            # Stop then start a plugin
    %% plugins disable   Name-Vsn            # Disable auto-boot
    %% plugins enable    Name-Vsn [Position] # Enable auto-boot at Position in the boot list, where Position could be
    %%                                       # 'front', 'rear', or 'before Other-Vsn' to specify a relative position.
    %%                                       # The Position parameter can be used to adjust the boot order.
    %%                                       # If no Position is given, an already configured plugin
    %%                                       # will stay at is old position; a newly plugin is appended to the rear
    %%                                       # e.g. plugins disable foo-0.1.0 front
    %%                                       #      plugins enable bar-0.2.0 before foo-0.1.0
    ok.

t_vm(_Config) ->
    %% vm all     # Show info of Erlang VM
    emqx_ctl:run_command(["vm", "all"]),
    %% vm load    # Show load of Erlang VM
    emqx_ctl:run_command(["vm", "load"]),
    %% vm memory  # Show memory of Erlang VM
    emqx_ctl:run_command(["vm", "memory"]),
    %% vm process # Show process of Erlang VM
    emqx_ctl:run_command(["vm", "process"]),
    %% vm io      # Show IO of Erlang VM
    emqx_ctl:run_command(["vm", "io"]),
    %% vm ports   # Show Ports of Erlang VM
    emqx_ctl:run_command(["vm", "ports"]),
    ok.

t_mnesia(_Config) ->
    %% mnesia # Mnesia system info
    emqx_ctl:run_command(["mnesia"]),
    ok.

t_log(_Config) ->
    %% log set-level <Level>                      # Set the overall log level
    %% log primary-level                          # Show the primary log level now
    emqx_ctl:run_command(["log", "primary-level"]),
    %% log primary-level <Level>                  # Set the primary log level
    %% log handlers list                          # Show log handlers
    emqx_ctl:run_command(["log", "handlers", "list"]),
    %% log handlers start <HandlerId>             # Start a log handler
    %% log handlers stop  <HandlerId>             # Stop a log handler
    %% log handlers set-level <HandlerId> <Level> # Set log level of a log handler
    ok.

t_trace(_Config) ->
    %% trace list                                        # List all traces started on local node
    emqx_ctl:run_command(["trace", "list"]),
    %% trace start client <ClientId> <File> [<Level>]    # Traces for a client on local node
    %% trace stop  client <ClientId>                     # Stop tracing for a client on local node
    %% trace start topic  <Topic>    <File> [<Level>]    # Traces for a topic on local node
    %% trace stop  topic  <Topic>                        # Stop tracing for a topic on local node
    %% trace start ip_address  <IP>    <File> [<Level>]  # Traces for a client ip on local node
    %% trace stop  ip_addresss  <IP>                     # Stop tracing for a client ip on local node
    ok.

t_traces(_Config) ->
    %% traces list                             # List all cluster traces started
    emqx_ctl:run_command(["traces", "list"]),
    %% traces start <Name> client <ClientId>   # Traces for a client in cluster
    %% traces start <Name> topic <Topic>       # Traces for a topic in cluster
    %% traces start <Name> ip_address <IPAddr> # Traces for a IP in cluster
    %% traces stop  <Name>                     # Stop trace in cluster
    %% traces delete  <Name>                   # Delete trace in cluster
    ok.

t_listeners(_Config) ->
    %% listeners                      # List listeners
    emqx_ctl:run_command(["listeners"]),
    %% listeners stop    <Identifier> # Stop a listener
    %% listeners start   <Identifier> # Start a listener
    %% listeners restart <Identifier> # Restart a listener
    ok.

t_authz(_Config) ->
    %% authz cache-clean all         # Clears authorization cache on all nodes
    emqx_ctl:run_command(["authz", "cache-clean", "all"]),
    %% authz cache-clean node <Node> # Clears authorization cache on given node
    %% authz cache-clean <ClientId>  # Clears authorization cache for given client
    ok.

t_olp(_Config) ->
    %% olp status  # Return OLP status if system is overloaded
    emqx_ctl:run_command(["olp", "status"]),
    %% olp enable  # Enable overload protection
    %% olp disable # Disable overload protection
    ok.

t_admin(_Config) ->
    %% admins add <Username> <Password> <Description> # Add dashboard user
    %% admins passwd <Username> <Password>            # Reset dashboard user password
    %% admins del <Username>                          # Delete dashboard user
    ok.
