defmodule Mix.Tasks.Emqx.GenUmbrellaApps do
  @moduledoc """
  This simply outputs the list of umbrella apps so that we can decide whether to enable
  such application when a feature gate is enabled in `emqx_machine`.

  ## Usage

      mix emqx.gen_umbrella_apps

  ## Options

  None currently supported.
  """

  use Mix.Task

  @shortdoc "Generate umbrella apps file (umbrella_apps.txt)"

  @requirements []

  @impl Mix.Task
  def run(_args) do
    output_file = "apps/emqx_machine/priv/umbrella_apps.txt"

    umbrella_apps =
      Mix.Dep.Umbrella.cached()
      |> Enum.map(& &1.app)
      |> Enum.sort()

    File.open!(output_file, [:write], fn fd ->
      Enum.each(umbrella_apps, fn app ->
        IO.write(fd, "#{app}\n")
      end)
    end)

    :ok
  end
end
