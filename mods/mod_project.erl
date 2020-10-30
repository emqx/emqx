-module(mod_project).

-export([ get_vsn/1
        , coveralls_configs/1
        ]).

get_vsn(_Conf) ->
  PkgVsn = case os:getenv("PKG_VSN") of
    false -> error({env_undefined, "PKG_VSN"});
    Vsn -> Vsn
  end,
  re:replace(PkgVsn, "v", "", [{return ,list}]).

coveralls_configs(_Config) ->
    case {os:getenv("GITHUB_ACTIONS"), os:getenv("GITHUB_TOKEN")} of
    {"true", Token} when is_list(Token) ->
        CONFIG1 = [
            {coveralls_repo_token, Token},
            {coveralls_service_job_id, os:getenv("GITHUB_RUN_ID")},
            {coveralls_commit_sha, os:getenv("GITHUB_SHA")},
            {coveralls_service_number, os:getenv("GITHUB_RUN_NUMBER")},
            {coveralls_coverdata, "_build/test/cover/*.coverdata"},
            {coveralls_service_name, "github"}
        ],
        case os:getenv("GITHUB_EVENT_NAME") =:= "pull_request"
            andalso string:tokens(os:getenv("GITHUB_REF"), "/") of
            [_, "pull", PRNO, _] ->
                [{coveralls_service_pull_request, PRNO} | CONFIG1];
            _ ->
                CONFIG1
        end;
    _ ->
        []
    end.
