%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 30. 8月 2022 下午11:12
%%%-------------------------------------------------------------------
-module(alinkdata_post_service).
-author("yqfclid").

%% API
-export([
    query_post/4,
    add_post/4,
    edit_post/4,
    remove_post/4,
    export/4
]).

%%%===================================================================
%%% API
%%%===================================================================
query_post(_OperationID, Args, _Context, Req) ->
    Res =
        case alinkdata_dao:query_no_count(select_post_by_id, Args) of
            {ok, [Post]} ->
                alinkdata_ajax_result:success_result(Post);
            {ok, []} ->
                alinkdata_ajax_result:error_result(<<"not found">>);
            {error, Reason} ->
                alinkdata_ajax_result:error_result(Reason)
        end,
    alinkdata_common_service:response(Res, Req).


add_post(_OperationID, Args, #{token := Token} = _Context, Req) ->
    Check =
        case check_post_name_unique(Args) of
            ok ->
                check_post_code_unique(Args);
            {error, Reason} ->
                {error, Reason}
        end,
    Res =
        case Check of
            ok ->
                #{<<"user">> := #{<<"userName">> := SelfUserName}} = ehttpd_auth:get_session(Token),
                NArgs = Args#{<<"createBy">> => SelfUserName},
                ok = alinkdata_dao:query_no_count(insert_post, NArgs),
                alinkdata_ajax_result:success_result();
            {error, FReason} ->
                alinkdata_ajax_result:error_result(FReason)
        end,
    alinkdata_common_service:response(Res, Req).


edit_post(_OperationID, Args, #{token := Token} = _Context, Req) ->
    Check =
        case check_post_name_unique(Args) of
            ok ->
                check_post_code_unique(Args);
            {error, Reason} ->
                {error, Reason}
        end,
    Res =
        case Check of
            ok ->
                #{<<"user">> := #{<<"userName">> := SelfUserName}} = ehttpd_auth:get_session(Token),
                ok = alinkdata_dao:query_no_count(update_post, Args#{<<"updateBy">> => SelfUserName}),
                alinkdata_ajax_result:success_result();
            {error, FReason} ->
                alinkdata_ajax_result:error_result(FReason)
        end,
    alinkdata_common_service:response(Res, Req).


remove_post(_OperationID, Args, _Context, Req) ->
    PostIds = binary:split(maps:get(<<"postId">>, Args, []), <<",">>, [global]),
    Check =
        lists:foldl(
            fun(PostId, ok) ->
                {ok, [PostInfo]} = alinkdata_dao:query_no_count(select_post_by_id, #{<<"postId">> => PostId}),
                    case alinkdata_dao:query_no_count(count_user_post_by_post_id, #{<<"postId">> => PostId}) of
                    {ok, [#{<<"count(1)">> := 0}]} ->
                        ok;
                    {ok, _} ->
                        PostName = maps:get(<<"postName">>, PostInfo, <<>>),
                        {error, <<PostName/binary, "已分配,不能删除"/utf8>>};
                    {error, Reason} ->
                        {error, Reason}
                end;
                (_, {error, Reason}) ->
                    {error, Reason}
            end , ok, PostIds),
    Res =
        case Check of
            ok ->
                ok = alinkdata_dao:query_no_count(delete_post_by_ids, #{<<"postIds">> => PostIds}),
                alinkdata_ajax_result:success_result();
            {error, FReason} ->
                alinkdata_ajax_result:error_result(FReason)
        end,
    alinkdata_common_service:response(Res, Req).


export(_OperationID, Args, _Context, _Req) ->
    {ok,Posts} = alinkdata_dao:query_no_count(select_post_list, Args),
    Content = alinkdata_common:to_file_content(Posts),
    Headers = alinkdata_common:file_headers(Content, <<"\"post_data.csv\""/utf8>>),
    {200, Headers, Content}.
%%%===================================================================
%%% Internal functions
%%%===================================================================
check_post_name_unique(Args) ->
    PostId  = maps:get(<<"postId">>, Args, -1),
    case alinkdata_dao:query_no_count(check_post_name_unique, Args) of
        {ok, []} ->
            ok;
        {ok, [#{<<"postId">> := PostId}]} ->
            ok;
        {ok, _} ->
            PostName = maps:get(<<"postName">>, Args, <<>>),
            {error, <<"新增岗位'"/utf8, PostName/binary, "'失败，岗位名称已存在"/utf8>>};
        {error, Reason} ->
            {error, Reason}
    end.


check_post_code_unique(Args) ->
    PostId  = maps:get(<<"postId">>, Args, -1),
    case alinkdata_dao:query_no_count(check_post_code_unique, Args) of
        {ok, []} ->
            ok;
        {ok, [#{<<"postId">> := PostId}]} ->
            ok;
        {ok, _} ->
            PostCode = maps:get(<<"postCode">>, Args, <<>>),
            {error, <<"新增岗位'"/utf8, PostCode/binary, "'失败，岗位编码已存在"/utf8>>};
        {error, Reason} ->
            {error, Reason}
    end.