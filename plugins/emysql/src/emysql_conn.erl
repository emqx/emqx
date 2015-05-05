%%% File    : emysql_conn.erl
%%% Author  : Ery Lee
%%% Purpose : connection of mysql driver
%%% Created : 11 Jan 2010 
%%% License : http://www.opengoss.com
%%%
%%% Copyright (C) 2012, www.opengoss.com 
%%%----------------------------------------------------------------------
-module(emysql_conn).

-include("emysql.hrl").

-import(proplists, [get_value/2, get_value/3]).

-behaviour(gen_server).

%% External exports
-export([start_link/2,
		info/1,
		sqlquery/2,
		sqlquery/3,
		prepare/3,
		execute/3,
		execute/4,
		unprepare/2]).

%% Callback
-export([init/1, 
		handle_call/3, 
		handle_cast/2, 
		handle_info/2, 
		terminate/2, 
		code_change/3]).

-record(state, {
		id,
		host,
		port,
		user,
		password,
		database,
		encoding,
		mysql_version,
		recv_pid,
		socket,
		data}).

%%-define(KEEPALIVE_QUERY, <<"SELECT 1;">>).

-define(SECURE_CONNECTION, 32768).

-define(MYSQL_QUERY_OP, 3).

%CALL > CONNECT
-define(CALL_TIMEOUT, 301000).

-define(CONNECT_TIMEOUT, 300000).

-define(MYSQL_4_0, 40). %% Support for MySQL 4.0.x

-define(MYSQL_4_1, 41). %% Support for MySQL 4.1.x et 5.0.x

%%--------------------------------------------------------------------
%% Function: start(Opts)
%% Descrip.: Starts a mysql_conn process that connects to a MySQL
%%           server, logs in and chooses a database.
%% Returns : {ok, Pid} | {error, Reason}
%%           Pid    = pid()
%%           Reason = string()
%%--------------------------------------------------------------------
start_link(Id, Opts) ->
    gen_server:start_link(?MODULE, [Id, Opts], []).

info(Conn) ->
	gen_server:call(Conn, info).

%%--------------------------------------------------------------------
%% Function: sqlquery(Query)
%%           Queries   = A single binary() query or a list of binary() queries.
%%                     If a list is provided, the return value is the return
%%                     of the last query, or the first query that has
%%                     returned an error. If an error occurs, execution of
%%                     the following queries is aborted.
%%           From    = pid() or term(), use a From of self() when
%%                     using this module for a single connection,
%%                     or pass the gen_server:call/3 From argument if
%%                     using a gen_server to do the querys (e.g. the
%%                     mysql_dispatcher)
%%           Timeout = integer() | infinity, gen_server timeout value
%% Descrip.: Send a query or a list of queries and wait for the result
%%           if running stand-alone (From = self()), but don't block
%%           the caller if we are not running stand-alone
%%           (From = gen_server From).
%% Returns : ok                        | (non-stand-alone mode)
%%           {data, #mysql_result}     | (stand-alone mode)
%%           {updated, #mysql_result}  | (stand-alone mode)
%%           {error, #mysql_result}      (stand-alone mode)
%%           FieldInfo = term()
%%           Rows      = list() of [string()]
%%           Reason    = term()
%%--------------------------------------------------------------------
sqlquery(Conn, Query) ->
    sqlquery(Conn, Query, ?CALL_TIMEOUT).

sqlquery(Conn, Query, Timeout)  ->
    call(Conn, {sqlquery, Query}, Timeout).

prepare(Conn, Name, Stmt) ->
    call(Conn, {prepare, Name, Stmt}).

execute(Conn, Name, Params) ->
    execute(Conn, Name, Params, ?CALL_TIMEOUT).

execute(Conn, Name, Params, Timeout) ->
    call(Conn, {execute, Name, Params}, Timeout).

unprepare(Conn, Name) ->
    call(Conn, {unprepare, Name}).

%%--------------------------------------------------------------------
%% Function: init(Host, Port, User, Password, Database, Parent)
%%           Host     = string()
%%           Port     = integer()
%%           User     = string()
%%           Password = string()
%%           Database = string()
%%           Parent   = pid() of process starting this mysql_conn
%% Descrip.: Connect to a MySQL server, log in and chooses a database.
%%           Report result of this to Parent, and then enter loop() if
%%           we were successfull.
%% Returns : void() | does not return
%%--------------------------------------------------------------------
init([Id, Opts]) ->
	put(queries, 0),
    Host = get_value(host, Opts, "localhost"),
    Port = get_value(port, Opts, 3306),
    UserName = get_value(username, Opts, "root"),
    Password = get_value(password, Opts, "public"),
    Database = get_value(database, Opts),
    Encoding = get_value(encoding, Opts, utf8),
	case emysql_recv:start_link(Host, Port) of
	{ok, RecvPid, Sock} ->
	    case mysql_init(Sock, RecvPid, UserName, Password) of
		{ok, Version} ->
		    Db = iolist_to_binary(Database),
		    case do_query(Sock, RecvPid, <<"use ", Db/binary>>, Version) of
			{error, #mysql_result{error = Error} = _MySQLRes} ->
			    error_logger:error_msg("emysql_conn: use '~p' error: ~p", [Database, Error]),
                {stop, using_db_error};
			{_ResultType, _MySQLRes} ->
				emysql:pool(Id), %pool it
				pg2:create(emysql_conn),
				pg2:join(emysql_conn, self()),
                EncodingBinary = list_to_binary(atom_to_list(Encoding)),
                do_query(Sock, RecvPid, <<"set names '", EncodingBinary/binary, "'">>, Version),
                State = #state{
						id = Id,
                        host = Host, 
                        port = Port, 
                        user = UserName, 
                        password = Password,
                        database = Database, 
                        encoding = Encoding, 
                        mysql_version = Version,
                        recv_pid = RecvPid,
                        socket   = Sock,
                        data     = <<>>},
			    {ok, State}
            end;
		{error, Reason} ->
            {stop, {login_failed, Reason}}
        end;
	{error, Reason} ->
		{stop, Reason}
	end.

handle_call(info, _From, #state{id = Id} = State) ->
	Reply = {Id, self(), get(queries)},
	{reply, Reply, State};

handle_call({sqlquery, Query}, _From, #state{socket = Socket, 
        recv_pid = RecvPid, mysql_version = Ver} = State)  ->
	put(queries, get(queries) + 1),
    case do_query(Socket, RecvPid, Query, Ver) of
    {error, mysql_timeout} = Err ->
        {stop, mysql_timeout, Err, State};
    Res -> 
        {reply, Res, State}
    end;

handle_call({prepare, Name, Stmt}, _From, #state{socket = Socket, 
	recv_pid = RecvPid, mysql_version = Ver} = State) ->

    case do_prepare(Socket, RecvPid, Name, Stmt, Ver) of
    {error, mysql_timeout} -> 
        {stop, mysql_timeout, State};
    _ ->
        {reply, ok, State}
    end;

handle_call({unprepare, Name}, _From, #state{socket = Socket, 
        recv_pid = RecvPid, mysql_version = Ver} = State) ->
    case do_unprepare(Socket, RecvPid, Name, Ver) of
    {error, mysql_timeout} -> 
        {stop, mysql_timeout, State};
    _ ->
        {reply, ok, State}
    end;

handle_call({execute, Name, Params}, _From, #state{socket = Socket, 
        recv_pid = RecvPid, mysql_version = Ver} = State) ->
    case do_execute(Socket, RecvPid, Name, Params, Ver) of
    {error, mysql_timeout} = Err ->
        {stop, mysql_timeout, Err, State};
    Res -> 
        {reply, Res, State}
    end;

handle_call(Req, _From, State) ->
    error_logger:error_msg("badreq to emysql_conn: ~p", [Req]),
    {reply, {error, badreq}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({mysql_recv, _RecvPid, data, _Packet, SeqNum}, State) ->
    error_logger:error_msg("unexpected mysql_recv: seq_num = ~p", [SeqNum]),
    {noreply, State};

handle_info({mysql_recv, _RecvPid, closed, E}, State) ->
    error_logger:error_msg("mysql socket closed: ~p", [E]),
    {stop, socket_closed, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

do_queries(Sock, RecvPid, Queries, Version) ->
    catch
	lists:foldl(
	  fun(Query, _LastResponse) ->
		  case do_query(Sock, RecvPid, Query, Version) of
		      {error, _} = Err -> throw(Err);
		      Res -> Res
		  end
	  end, ok, Queries).

do_query(Sock, RecvPid, Query, Version) ->
    Query1 = iolist_to_binary(Query),
    %?DEBUG("sqlquery ~p (id ~p)", [Query1, RecvPid]),
    Packet = <<?MYSQL_QUERY_OP, Query1/binary>>,
    case do_send(Sock, Packet, 0) of
	ok ->
	    get_query_response(RecvPid, Version);
	{error, Reason} ->
	    {error, Reason}
    end.

do_prepare(Socket, RecvPid, Name, Stmt, Ver) ->
    NameBin = atom_to_binary(Name),
    StmtBin = <<"PREPARE ", NameBin/binary, " FROM '", Stmt/binary, "'">>,
    do_query(Socket, RecvPid, StmtBin, Ver).

do_execute(Socket, RecvPid, Name, Params, Ver) ->
    Stmts = make_statements(Name, Params),
    do_queries(Socket, RecvPid, Stmts, Ver).

do_unprepare(Socket, RecvPid, Name, Ver) ->
    NameBin = atom_to_binary(Name),
    StmtBin = <<"UNPREPARE ", NameBin/binary>>,
    do_query(Socket, RecvPid, StmtBin, Ver).

make_statements(Name, []) ->
    NameBin = atom_to_binary(Name),
    [<<"EXECUTE ", NameBin/binary>>];

make_statements(Name, Params) ->
    NumParams = length(Params),
    ParamNums = lists:seq(1, NumParams),
    NameBin = atom_to_binary(Name),
    ParamNames =
	lists:foldl(
	  fun(Num, Acc) ->
		  ParamName = [$@ | integer_to_list(Num)],
		  if Num == 1 ->
			  ParamName ++ Acc;
		     true ->
			  [$, | ParamName] ++ Acc
		  end
	  end, [], lists:reverse(ParamNums)),
    ParamNamesBin = list_to_binary(ParamNames),
    ExecStmt = <<"EXECUTE ", NameBin/binary, " USING ",
		ParamNamesBin/binary>>,

    ParamVals = lists:zip(ParamNums, Params),
    Stmts = lists:foldl(
	      fun({Num, Val}, Acc) ->
		      NumBin = emysql:encode(Num, true),
		      ValBin = emysql:encode(Val, true),
		      [<<"SET @", NumBin/binary, "=", ValBin/binary>> | Acc]
	       end, [ExecStmt], lists:reverse(ParamVals)),
    Stmts.

atom_to_binary(Val) ->
    <<_:4/binary, Bin/binary>> = term_to_binary(Val),
    Bin.

%%--------------------------------------------------------------------
%% authentication
%%--------------------------------------------------------------------
do_old_auth(Sock, RecvPid, SeqNum, User, Password, Salt1) ->
    Auth = emysql_auth:password_old(Password, Salt1),
    Packet = emysql_auth:make_auth(User, Auth),
    do_send(Sock, Packet, SeqNum),
    do_recv(RecvPid, SeqNum).

do_new_auth(Sock, RecvPid, SeqNum, User, Password, Salt1, Salt2) ->
    Auth = emysql_auth:password_new(Password, Salt1 ++ Salt2),
    Packet2 = emysql_auth:make_new_auth(User, Auth, none),
    do_send(Sock, Packet2, SeqNum),
    case do_recv(RecvPid, SeqNum) of
    {ok, Packet3, SeqNum2} ->
        case Packet3 of
        <<254:8>> ->
            AuthOld = emysql_auth:password_old(Password, Salt1),
            do_send(Sock, <<AuthOld/binary, 0:8>>, SeqNum2 + 1),
            do_recv(RecvPid, SeqNum2 + 1);
        _ -> 
            {ok, Packet3, SeqNum2}
        end;
    {error, Reason} ->
        {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Function: mysql_init(Sock, RecvPid, User, Password)
%%           Sock     = term(), gen_tcp socket
%%           RecvPid  = pid(), mysql_recv process
%%           User     = string()
%%           Password = string()
%%           LogFun   = undefined | function() with arity 3
%% Descrip.: Try to authenticate on our new socket.
%% Returns : ok | {error, Reason}
%%           Reason = string()
%%--------------------------------------------------------------------
mysql_init(Sock, RecvPid, User, Password) ->
    case do_recv(RecvPid, undefined) of
	{ok, Packet, InitSeqNum} ->
	    {Version, Salt1, Salt2, Caps} = greeting(Packet),
        %?DEBUG("version: ~p, ~p, ~p, ~p", [Version, Salt1, Salt2, Caps]),
	    AuthRes =
		case Caps band ?SECURE_CONNECTION of
        ?SECURE_CONNECTION ->
			do_new_auth(Sock, RecvPid, InitSeqNum + 1, User, Password, Salt1, Salt2);
        _ ->
			do_old_auth(Sock, RecvPid, InitSeqNum + 1, User, Password, Salt1)
		end,
	    case AuthRes of
		{ok, <<0:8, _Rest/binary>>, _RecvNum} ->
		    {ok,Version};
		{ok, <<255:8, _Code:16/little, Message/binary>>, _RecvNum} ->
		    {error, binary_to_list(Message)};
		{ok, RecvPacket, _RecvNum} ->
		    {error, binary_to_list(RecvPacket)};
		{error, Reason} ->
		    %?ERROR("init failed receiving data : ~p", [Reason]),
		    {error, Reason}
	    end;
	{error, Reason} ->
	    {error, Reason}
    end.

greeting(Packet) ->
    <<_Protocol:8, Rest/binary>> = Packet,
    {Version, Rest2} = asciz(Rest),
    <<_TreadID:32/little, Rest3/binary>> = Rest2,
    {Salt, Rest4} = asciz(Rest3),
    <<Caps:16/little, Rest5/binary>> = Rest4,
    <<_ServerChar:16/binary-unit:8, Rest6/binary>> = Rest5,
    {Salt2, _Rest7} = asciz(Rest6),
    %?DEBUG("greeting version ~p (protocol ~p) salt ~p caps ~p serverchar ~p"
	  %"salt2 ~p",
	  %[Version, Protocol, Salt, Caps, ServerChar, Salt2]),
    {normalize_version(Version), Salt, Salt2, Caps}.

%% part of greeting/2
asciz(Data) when is_binary(Data) ->
    asciz_binary(Data, []);
asciz(Data) when is_list(Data) ->
    {String, [0 | Rest]} = lists:splitwith(fun (C) ->
						   C /= 0
					   end, Data),
    {String, Rest}.

%% @doc Find the first zero-byte in Data and add everything before it
%%   to Acc, as a string.
%%
%% @spec asciz_binary(Data::binary(), Acc::list()) ->
%%   {NewList::list(), Rest::binary()}
asciz_binary(<<>>, Acc) ->
    {lists:reverse(Acc), <<>>};
asciz_binary(<<0:8, Rest/binary>>, Acc) ->
    {lists:reverse(Acc), Rest};
asciz_binary(<<C:8, Rest/binary>>, Acc) ->
    asciz_binary(Rest, [C | Acc]).

%%--------------------------------------------------------------------
%% Function: get_query_response(RecvPid)
%%           RecvPid = pid(), mysql_recv process
%%           Version = integer(), Representing MySQL version used
%% Descrip.: Wait for frames until we have a complete query response.
%% Returns :   {data, #mysql_result}
%%             {updated, #mysql_result}
%%             {error, #mysql_result}
%%           FieldInfo    = list() of term()
%%           Rows         = list() of [string()]
%%           AffectedRows = int()
%%           Reason       = term()
%%--------------------------------------------------------------------
get_query_response(RecvPid, Version) ->
    case do_recv(RecvPid, undefined) of
	{ok, <<Fieldcount:8, Rest/binary>>, _} ->
	    case Fieldcount of
		0 ->
		    %% No Tabular data
            {AffectedRows, Rest1} = decode_length_binary(Rest),
            {InsertId, _} = decode_length_binary(Rest1),
		    {updated, #mysql_result{insert_id = InsertId, affectedrows=AffectedRows}};
		255 ->
		    <<_Code:16/little, Message/binary>>  = Rest,
		    {error, #mysql_result{error=Message}};
		_ ->
		    %% Tabular data received
		    case get_fields(RecvPid, [], Version) of
			{ok, Fields} ->
			    case get_rows(Fields, RecvPid, []) of
				{ok, Rows} ->
				    {data, #mysql_result{fieldinfo=Fields,
							 rows=Rows}};
				{error, Reason} ->
				    {error, Reason}
			    end;
			{error, Reason} ->
			    {error, Reason}
		    end
	    end;
	{error, Reason} ->
	    {error, Reason}
    end.

decode_length_binary(<<Len:8, Rest/binary>>) ->
    if
    Len =< 251 -> 
        {Len, Rest};
    Len == 252 -> %two bytes
        <<Val:16/little, Rest1/binary>> = Rest,
        {Val, Rest1};
    Len == 253 -> %three
        <<Val:24/little, Rest1/binary>> = Rest,
        {Val, Rest1};
    Len == 254 -> %eight
        <<Val:64/little, Rest1/binary>> = Rest,
        {Val, Rest1};
    true ->
        %?ERROR("affectedrows: ~p", [Len]),
        {0, Rest}
    end.

%%--------------------------------------------------------------------
%% Function: do_recv(RecvPid, SeqNum)
%%           RecvPid = pid(), mysql_recv process
%%           SeqNum  = undefined | integer()
%% Descrip.: Wait for a frame decoded and sent to us by RecvPid.
%%           Either wait for a specific frame if SeqNum is an integer,
%%           or just any frame if SeqNum is undefined.
%% Returns : {ok, Packet, Num} |
%%           {error, Reason}
%%           Reason = term()
%%
%% Note    : Only to be used externally by the 'mysql_auth' module.
%%--------------------------------------------------------------------
do_recv(RecvPid, SeqNum) when SeqNum == undefined ->
    receive
    {mysql_recv, RecvPid, data, Packet, Num} ->
	    {ok, Packet, Num};
	{mysql_recv, RecvPid, closed, _E} ->
	    {error, socket_closed}
    after ?CONNECT_TIMEOUT ->
        {error, mysql_timeout}
    end;

do_recv(RecvPid, SeqNum) when is_integer(SeqNum) ->
    ResponseNum = SeqNum + 1,
    receive
    {mysql_recv, RecvPid, data, Packet, ResponseNum} ->
	    {ok, Packet, ResponseNum};
	{mysql_recv, RecvPid, closed, _E} ->
	    {error, socket_closed}
    after ?CONNECT_TIMEOUT ->
        {error, mysql_timeout}
    end.

call(Conn, Req) ->
    gen_server:call(Conn, Req).

call(Conn, Req, Timeout) ->
    gen_server:call(Conn, Req, Timeout).

%%--------------------------------------------------------------------
%% Function: get_fields(RecvPid, [], Version)
%%           RecvPid = pid(), mysql_recv process
%%           Version = integer(), Representing MySQL version used
%% Descrip.: Received and decode field information.
%% Returns : {ok, FieldInfo} |
%%           {error, Reason}
%%           FieldInfo = list() of term()
%%           Reason    = term()
%%--------------------------------------------------------------------
%% Support for MySQL 4.0.x:
get_fields(RecvPid, Res, ?MYSQL_4_0) ->
    case do_recv(RecvPid, undefined) of
	{ok, Packet, _Num} ->
	    case Packet of
		<<254:8>> ->
		    {ok, lists:reverse(Res)};
		<<254:8, Rest/binary>> when size(Rest) < 8 ->
		    {ok, lists:reverse(Res)};
		_ ->
		    {Table, Rest} = get_with_length(Packet),
		    {Field, Rest2} = get_with_length(Rest),
		    {LengthB, Rest3} = get_with_length(Rest2),
		    LengthL = size(LengthB) * 8,
		    <<Length:LengthL/little>> = LengthB,
		    {Type, Rest4} = get_with_length(Rest3),
		    {_Flags, _Rest5} = get_with_length(Rest4),
		    This = {Table,
			    Field,
			    Length,
			    %% TODO: Check on MySQL 4.0 if types are specified
			    %%       using the same 4.1 formalism and could 
			    %%       be expanded to atoms:
			    Type},
		    get_fields(RecvPid, [This | Res], ?MYSQL_4_0)
	    end;
	{error, Reason} ->
	    {error, Reason}
    end;
%% Support for MySQL 4.1.x and 5.x:
get_fields(RecvPid, Res, ?MYSQL_4_1) ->
    case do_recv(RecvPid, undefined) of
	{ok, Packet, _Num} ->
	    case Packet of
		<<254:8>> ->
		    {ok, lists:reverse(Res)};
		<<254:8, Rest/binary>> when size(Rest) < 8 ->
		    {ok, lists:reverse(Res)};
		_ ->
		    {_Catalog, Rest} = get_with_length(Packet),
		    {_Database, Rest2} = get_with_length(Rest),
		    {Table, Rest3} = get_with_length(Rest2),
		    %% OrgTable is the real table name if Table is an alias
		    {_OrgTable, Rest4} = get_with_length(Rest3),
		    {Field, Rest5} = get_with_length(Rest4),
		    %% OrgField is the real field name if Field is an alias
		    {_OrgField, Rest6} = get_with_length(Rest5),

		    <<_Metadata:8/little, _Charset:16/little,
		     Length:32/little, Type:8/little,
		     _Flags:16/little, _Decimals:8/little,
		     _Rest7/binary>> = Rest6,
		    
		    This = {Table,
			    Field,
			    Length,
			    get_field_datatype(Type)},
		    get_fields(RecvPid, [This | Res], ?MYSQL_4_1)
	    end;
	{error, Reason} ->
	    {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Function: get_rows(N, RecvPid, [])
%%           N       = integer(), number of rows to get
%%           RecvPid = pid(), mysql_recv process
%% Descrip.: Receive and decode a number of rows.
%% Returns : {ok, Rows} |
%%           {error, Reason}
%%           Rows = list() of [string()]
%%--------------------------------------------------------------------
get_rows(Fields, RecvPid, Res) ->
    case do_recv(RecvPid, undefined) of
	{ok, Packet, _Num} ->
	    case Packet of
		<<254:8, Rest/binary>> when size(Rest) < 8 ->
		    {ok, lists:reverse(Res)};
		_ ->
		    {ok, This} = get_row(Fields, Packet, []),
		    get_rows(Fields, RecvPid, [This | Res])
	    end;
	{error, Reason} ->
	    {error, Reason}
    end.

%% part of get_rows/4
get_row([], _Data, Res) ->
    {ok, lists:reverse(Res)};
get_row([Field | OtherFields], Data, Res) ->
    {Col, Rest} = get_with_length(Data),
    This = case Col of
	       null ->
		   undefined;
	       _ ->
		   convert_type(Col, element(4, Field))
	   end,
    get_row(OtherFields, Rest, [This | Res]).

get_with_length(<<251:8, Rest/binary>>) ->
    {null, Rest};
get_with_length(<<252:8, Length:16/little, Rest/binary>>) ->
    split_binary(Rest, Length);
get_with_length(<<253:8, Length:24/little, Rest/binary>>) ->
    split_binary(Rest, Length);
get_with_length(<<254:8, Length:64/little, Rest/binary>>) ->
    split_binary(Rest, Length);
get_with_length(<<Length:8, Rest/binary>>) when Length < 251 ->
    split_binary(Rest, Length).


%%--------------------------------------------------------------------
%% Function: do_send(Sock, Packet, SeqNum)
%%           Sock   = term(), gen_tcp socket
%%           Packet = binary()
%%           SeqNum = integer(), packet sequence number
%% Descrip.: Send a packet to the MySQL server.
%% Returns : result of gen_tcp:send/2
%%--------------------------------------------------------------------
do_send(Sock, Packet, SeqNum) when is_binary(Packet), is_integer(SeqNum) ->
    Data = <<(size(Packet)):24/little, SeqNum:8, Packet/binary>>,
    gen_tcp:send(Sock, Data).

%%--------------------------------------------------------------------
%% Function: normalize_version(Version)
%%           Version  = string()
%% Descrip.: Return a flag corresponding to the MySQL version used.
%%           The protocol used depends on this flag.
%% Returns : Version = string()
%%--------------------------------------------------------------------
normalize_version([$4,$.,$0|_T]) ->
    %?DEBUG("switching to MySQL 4.0.x protocol.", []),
    ?MYSQL_4_0;
normalize_version([$4,$.,$1|_T]) ->
    ?MYSQL_4_1;
normalize_version([$5|_T]) ->
    %% MySQL version 5.x protocol is compliant with MySQL 4.1.x:
    ?MYSQL_4_1; 
normalize_version([$6|_T]) ->
    %% MySQL version 6.x protocol is compliant with MySQL 4.1.x:
    ?MYSQL_4_1; 
normalize_version(_Other) ->
    %?ERROR("MySQL version '~p' not supported: MySQL Erlang module "
	% "might not work correctly.", [Other]),
    %% Error, but trying the oldest protocol anyway:
    ?MYSQL_4_0.

%%--------------------------------------------------------------------
%% Function: get_field_datatype(DataType)
%%           DataType = integer(), MySQL datatype
%% Descrip.: Return MySQL field datatype as description string
%% Returns : String, MySQL datatype
%%--------------------------------------------------------------------
get_field_datatype(0) ->   'DECIMAL';
get_field_datatype(1) ->   'TINY';
get_field_datatype(2) ->   'SHORT';
get_field_datatype(3) ->   'LONG';
get_field_datatype(4) ->   'FLOAT';
get_field_datatype(5) ->   'DOUBLE';
get_field_datatype(6) ->   'NULL';
get_field_datatype(7) ->   'TIMESTAMP';
get_field_datatype(8) ->   'LONGLONG';
get_field_datatype(9) ->   'INT24';
get_field_datatype(10) ->  'DATE';
get_field_datatype(11) ->  'TIME';
get_field_datatype(12) ->  'DATETIME';
get_field_datatype(13) ->  'YEAR';
get_field_datatype(14) ->  'NEWDATE';
get_field_datatype(246) -> 'NEWDECIMAL';
get_field_datatype(247) -> 'ENUM';
get_field_datatype(248) -> 'SET';
get_field_datatype(249) -> 'TINYBLOB';
get_field_datatype(250) -> 'MEDIUM_BLOG';
get_field_datatype(251) -> 'LONG_BLOG';
get_field_datatype(252) -> 'BLOB';
get_field_datatype(253) -> 'VAR_STRING';
get_field_datatype(254) -> 'STRING';
get_field_datatype(255) -> 'GEOMETRY'.

convert_type(Val, ColType) ->
    case ColType of
	T when T == 'TINY';
	       T == 'SHORT';
	       T == 'LONG';
	       T == 'LONGLONG';
	       T == 'INT24';
	       T == 'YEAR' ->
	    list_to_integer(binary_to_list(Val));
	T when T == 'TIMESTAMP';
	       T == 'DATETIME' ->
	    {ok, [Year, Month, Day, Hour, Minute, Second], _Leftovers} =
		io_lib:fread("~d-~d-~d ~d:~d:~d", binary_to_list(Val)),
	    {datetime, {{Year, Month, Day}, {Hour, Minute, Second}}};
	'TIME' ->
	    {ok, [Hour, Minute, Second], _Leftovers} =
		io_lib:fread("~d:~d:~d", binary_to_list(Val)),
	    {time, {Hour, Minute, Second}};
	'DATE' ->
	    {ok, [Year, Month, Day], _Leftovers} =
		io_lib:fread("~d-~d-~d", binary_to_list(Val)),
	    {date, {Year, Month, Day}};
	T when T == 'DECIMAL';
	       T == 'NEWDECIMAL';
	       T == 'FLOAT';
	       T == 'DOUBLE' ->
	    {ok, [Num], _Leftovers} =
		case io_lib:fread("~f", binary_to_list(Val)) of
		    {error, _} ->
			io_lib:fread("~d", binary_to_list(Val));
		    Res ->
			Res
		end,
	    Num;
	_Other ->
	    Val
    end.
