import Config

File.cwd! |> IO.inspect

config :mnesia, dir: '/tmp/mnesia'
