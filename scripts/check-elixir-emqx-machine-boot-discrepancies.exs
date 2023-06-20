#!/usr/bin/env elixir

defmodule CheckElixirEMQXMachineBootDiscrepancies do
  alias EMQXUmbrella.MixProject

  def main() do
    {:ok, _} = Application.ensure_all_started(:mix)

    File.cwd!()
    |> Path.join("mix.exs")
    |> Code.compile_file()

    inputs = MixProject.check_profile!()
    profile = Mix.env()
    # produce `rebar.config.rendered` to consult

    File.cwd!()
    |> Path.join("rebar3")
    |> System.cmd(["as", to_string(profile)],
      env: [{"DEBUG", "1"}]
    )

    mix_apps = mix_emqx_machine_applications(inputs.edition_type)
    rebar_apps = rebar_emqx_machine_applications(profile)
    {mix_missing, rebar_missing} = diff_apps(mix_apps, rebar_apps)

    if Enum.any?(mix_missing) do
      IO.puts(
        "For profile=#{profile}, edition=#{inputs.edition_type} " <>
          "rebar.config.erl has the following emqx_machine_boot_apps " <>
          "that are missing in mix.exs:"
      )

      IO.inspect(mix_missing, syntax_colors: [atom: :red])
    end

    if Enum.any?(rebar_missing) do
      IO.puts(
        "For profile=#{profile}, edition=#{inputs.edition_type} " <>
          "mix.exs has the following emqx_machine_boot_apps " <>
          "that are missing in rebar3.config.erl:"
      )

      IO.inspect(rebar_missing, syntax_colors: [atom: :red])
    end

    success? = Enum.empty?(mix_missing) and Enum.empty?(rebar_missing)

    if not success? do
      System.halt(1)
    else
      IO.puts(
        IO.ANSI.green() <>
          "Mix and Rebar emqx_machine_boot_apps OK!" <>
          IO.ANSI.reset()
      )
    end
  end

  defp mix_emqx_machine_applications(edition_type) do
    EMQXUmbrella.MixProject.emqx_machine_boot_apps(edition_type)
  end

  defp rebar_emqx_machine_applications(profile) do
    {:ok, props} =
      File.cwd!()
      |> Path.join("rebar.config.rendered")
      |> :file.consult()

    props[:profiles][profile][:relx][:overlay_vars][:emqx_machine_boot_apps]
    |> to_string()
    |> String.split(~r/,\s+/)
    |> Enum.map(&String.to_atom/1)
  end

  defp diff_apps(mix_apps, rebar_apps) do
    mix_missing = rebar_apps -- mix_apps
    rebar_missing = mix_apps -- rebar_apps
    {mix_missing, rebar_missing}
  end
end

CheckElixirEMQXMachineBootDiscrepancies.main()
