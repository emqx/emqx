#!/usr/bin/env elixir

defmodule CheckUmbrellaApps do
  @moduledoc """
  Verifies that quickly checks if the generated `apps/emqx_machine/priv/umbrella_apps.txt`
  file is consistent.
  """

  def run() do
    file = "apps/emqx_machine/priv/umbrella_apps.txt"

    file_apps =
      file
      |> File.read!()
      |> String.split("\n", trim: true)
      |> MapSet.new()

    dir_apps =
      "apps/*/mix.exs"
      |> Path.wildcard()
      |> MapSet.new(fn path ->
        [_, app, _] = Path.split(path)
        app
      end)

    missing = MapSet.difference(dir_apps, file_apps)
    stale = MapSet.difference(file_apps, dir_apps)
    missing? = MapSet.size(missing) > 0
    stale? = MapSet.size(stale) > 0

    if missing? do
      log([:red, "There are umbrella apps missing from #{file}:"])
      Enum.each(missing, &log([:red, "  - #{&1}"]))
    end

    if stale? do
      log([:red, "There are stale umbrella apps in #{file}:"])
      Enum.each(stale, &log([:red, "  - #{&1}"]))
    end

    if stale? || missing? do
      log([:red, "Run `mix emqx.gen_umbrella_apps` to update it"])
      :erlang.halt(1)
    else
      log([:green, "Umbrella apps file ok!"])
    end
  end

  defp log(what) do
    what
    |> IO.ANSI.format()
    |> IO.puts()
  end
end

CheckUmbrellaApps.run()
