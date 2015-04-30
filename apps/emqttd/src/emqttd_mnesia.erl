%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% emqttd mnesia.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_mnesia).

-author('feng@emqtt.io').

-include("emqttd.hrl").

-export([start/0, cluster/1]).

-export([create_table/2, copy_table/1]). 

start() ->
    case init_schema() of
        ok -> 
            ok;
        {error, {_Node, {already_exists, _Node}}} ->
            ok;
        {error, Reason} -> 
            lager:error("mnesia init_schema error: ~p", [Reason])
    end,
    ok = mnesia:start(),
    init_tables(),
    wait_for_tables().

%%------------------------------------------------------------------------------
%% @doc
%% @private
%% init mnesia schema.
%%
%% @end
%%------------------------------------------------------------------------------
init_schema() ->
    case mnesia:system_info(extra_db_nodes) of
        [] ->
            %% create schema
            mnesia:create_schema([node()]);
        __ ->
            ok
    end.

%%------------------------------------------------------------------------------
%% @doc
%% @private
%% init mnesia tables.
%%
%% @end
%%------------------------------------------------------------------------------
init_tables() ->
    case mnesia:system_info(extra_db_nodes) of
        [] ->
            create_tables();
        _ ->
            copy_tables()
    end.

%%------------------------------------------------------------------------------
%% @doc
%% @private
%% create tables.
%%
%% @end
%%------------------------------------------------------------------------------
create_tables() ->
    emqttd_util:apply_module_attributes(boot_mnesia).

create_table(Table, Attrs) ->
    case mnesia:create_table(Table, Attrs) of
        {atomic, ok} -> ok;
        {aborted, {already_exists, Table}} -> ok;
        Error -> Error
    end.

%%------------------------------------------------------------------------------
%% @doc
%% @private
%% copy tables.
%%
%% @end
%%------------------------------------------------------------------------------
copy_tables() ->
    emqttd_util:apply_module_attributes(copy_mnesia).

copy_table(Table) ->
    case mnesia:add_table_copy(Table, node(), ram_copies) of
        {atomic, ok} -> ok;
        {aborted, {already_exists, Table, _Node}} -> ok;
        {aborted, Error} -> Error
    end.

%%------------------------------------------------------------------------------
%% @doc
%% @private
%% wait for tables.
%%
%% @end
%%------------------------------------------------------------------------------
wait_for_tables() -> 
    %%TODO: is not right?
    %%lager:info("local_tables: ~p", [mnesia:system_info(local_tables)]),
    mnesia:wait_for_tables(mnesia:system_info(local_tables), infinity).

%%------------------------------------------------------------------------------
%% @doc
%% @private
%% Simple cluster with another nodes.
%%
%% @end
%%------------------------------------------------------------------------------
cluster(Node) ->
    %% stop mnesia 
    mnesia:stop(),
    ok = wait_for_mnesia(stop),
    %% delete mnesia
    ok = mnesia:delete_schema([node()]),
    %% start mnesia
    ok = mnesia:start(),
    %% connect with extra_db_nodes
    case mnesia:change_config(extra_db_nodes, [Node]) of
        {ok, []} -> 
            throw({error, failed_to_connect_extra_db_nodes});
        {ok, Nodes} ->
            case lists:member(Node, Nodes) of
                true -> lager:info("mnesia connected to extra_db_node '~s' successfully!", [Node]);
                false -> lager:error("mnesia failed to connect extra_db_node '~s'!", [Node])
            end

    end,
    copy_tables(),
    wait_for_tables().
 
wait_for_mnesia(stop) ->
    case mnesia:system_info(is_running) of
        no ->
            ok;
        stopping ->
            lager:info("Waiting for mnesia to stop..."),
            timer:sleep(1000),
            wait_for_mnesia(stop);
        yes ->
            {error, mnesia_unexpectedly_running};
        starting ->
            {error, mnesia_unexpectedly_starting}
    end.

