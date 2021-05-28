-module(log_tracer_schema).

-include_lib("typerefl/include/types.hrl").

-export([fields/1]).

-reflect_type([t_level/0, t_cache_logs_in/0]).

-type t_level() :: debug | info | notice | warning | error | critical | alert | emergency.

-type t_cache_logs_in() :: memory | file.

fields("config") ->
    [ {condition, fun condition/1}
    , {level, fun level/1}
    , {enable_cache, fun enable_cache/1}
    , {cache_logs_in, fun cache_logs_in/1}
    , {cache_log_dir, fun cache_log_dir/1}
    , {bulk, fun bulk/1}
    ];
fields(_) -> [].

condition(mapping) -> "config.condition";
condition(type) -> map();
condition(_) -> undefined.

level(mapping) -> "config.level";
level(type) -> t_level();
level(_) -> undefined.

enable_cache(mapping) -> "config.enable_cache";
enable_cache(type) -> boolean();
enable_cache(_) -> undefined.

cache_logs_in(mapping) -> "config.cache_logs_in";
cache_logs_in(type) -> t_cache_logs_in();
cache_logs_in(_) -> undefined.

cache_log_dir(mapping) -> "config.cache_log_dir";
cache_log_dir(type) -> typerefl:regexp_string("^(.*)$");
cache_log_dir(_) -> undefined.

bulk(mapping) -> "config.bulk";
bulk(type) -> typerefl:regexp_string("^[. 0-9]+(B|KB|MB|GB)$");
bulk(_) -> undefined.
