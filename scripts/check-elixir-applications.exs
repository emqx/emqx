#!/usr/bin/env elixir

defmodule CheckElixirApplications do
  @default_applications [:kernel, :stdlib, :sasl]

  def main() do
    {:ok, _} = Application.ensure_all_started(:mix)
    inputs = read_inputs()
    # produce `rebar.config.rendered` to consult
    profile = profile_of(inputs)

    File.cwd!()
    |> Path.join("rebar3")
    |> System.cmd(["as", to_string(profile)],
      env: [{"DEBUG", "1"}]
    )

    File.cwd!()
    |> Path.join("mix.exs")
    |> Code.compile_file()

    mix_apps = mix_applications(inputs.release_type)
    rebar_apps = rebar_applications(profile)
    results = diff_apps(mix_apps, rebar_apps)

    report_discrepancy(
      results[:missing_apps],
      "* There are missing applications in the Elixir release",
      fn %{app: app, mode: mode, after: last_app} ->
        IO.puts("  * #{app}: #{inspect(mode)} should be placed after #{inspect(last_app)}")
      end
    )

    report_discrepancy(
      results[:different_modes],
      "* There are applications with different application modes in the Elixir release",
      fn %{app: app, rebar_mode: rebar_mode, mix_mode: mix_mode} ->
        IO.puts(
          "  * #{inspect(app)} should have mode #{inspect(rebar_mode)}, but it has mode #{inspect(mix_mode)}"
        )
      end
    )

    report_discrepancy(
      results[:different_positions],
      "* There are applications in the Elixir release in the wrong order",
      fn %{app: app, mode: mode, after: last_app} ->
        IO.puts("  * #{app}: #{inspect(mode)} should be placed after #{inspect(last_app)}")
      end
    )

    success? =
      results
      |> Map.take([:missing_apps, :different_modes, :different_positions])
      |> Map.values()
      |> Enum.concat()
      |> Enum.empty?()

    if not success? do
      System.halt(1)
    else
      IO.puts(
        IO.ANSI.green() <>
          "Mix and Rebar applications OK!" <>
          IO.ANSI.reset()
      )
    end
  end

  defp mix_applications(release_type) do
    EMQXUmbrella.MixProject.applications(release_type)
  end

  defp rebar_applications(profile) do
    {:ok, props} =
      File.cwd!()
      |> Path.join("rebar.config.rendered")
      |> :file.consult()

    props[:profiles][profile][:relx]
    |> Enum.find(&(elem(&1, 0) == :release))
    |> elem(2)
    |> Enum.map(fn
      app when is_atom(app) ->
        {app, :permanent}

      {app, mode} ->
        {app, mode}
    end)
    |> Enum.reject(fn {app, _mode} ->
      # Elixir already includes those implicitly
      app in @default_applications
    end)
  end

  defp profile_of(%{
         release_type: release_type,
         package_type: package_type,
         edition_type: edition_type
       }) do
    case {release_type, package_type, edition_type} do
      {:cloud, :bin, :community} ->
        :emqx

      {:cloud, :pkg, :community} ->
        :"emqx-pkg"

      {:cloud, :bin, :enterprise} ->
        :"emqx-enterprise"

      {:cloud, :pkg, :enterprise} ->
        :"emqx-enterprise-pkg"

      {:edge, :bin, :community} ->
        :"emqx-edge"

      {:edge, :pkg, :community} ->
        :"emqx-edge-pkg"
    end
  end

  defp read_inputs() do
    release_type =
      read_enum_env_var(
        "EMQX_RELEASE_TYPE",
        [:cloud, :edge],
        :cloud
      )

    package_type =
      read_enum_env_var(
        "EMQX_PACKAGE_TYPE",
        [:bin, :pkg],
        :bin
      )

    edition_type =
      read_enum_env_var(
        "EMQX_EDITION_TYPE",
        [:community, :enterprise],
        :community
      )

    %{
      release_type: release_type,
      package_type: package_type,
      edition_type: edition_type
    }
  end

  defp read_enum_env_var(env_var, allowed_values, default_value) do
    case System.fetch_env(env_var) do
      :error ->
        default_value

      {:ok, raw_value} ->
        value =
          raw_value
          |> String.downcase()
          |> String.to_atom()

        if value not in allowed_values do
          Mix.raise("""
          Invalid value #{raw_value} for variable #{env_var}.
          Allowed values are: #{inspect(allowed_values)}
          """)
        end

        value
    end
  end

  defp diff_apps(mix_apps, rebar_apps) do
    app_names = Keyword.keys(rebar_apps)
    mix_apps = Keyword.filter(mix_apps, fn {app, _mode} -> app in app_names end)

    acc = %{
      mix_apps: mix_apps,
      missing_apps: [],
      different_positions: [],
      different_modes: [],
      last_app: nil
    }

    Enum.reduce(
      rebar_apps,
      acc,
      fn
        {rebar_app, rebar_mode}, acc = %{mix_apps: [], last_app: last_app} ->
          missing_app = %{
            app: rebar_app,
            mode: rebar_mode,
            after: last_app
          }

          acc
          |> Map.update!(:missing_apps, &[missing_app | &1])
          |> Map.put(:last_app, rebar_app)

        {rebar_app, rebar_mode},
        acc = %{mix_apps: [{mix_app, mix_mode} | rest], last_app: last_app} ->
          case {rebar_app, rebar_mode} do
            {^mix_app, ^mix_mode} ->
              acc
              |> Map.put(:mix_apps, rest)
              |> Map.put(:last_app, rebar_app)

            {^mix_app, _mode} ->
              different_mode = %{
                app: rebar_app,
                rebar_mode: rebar_mode,
                mix_mode: mix_mode
              }

              acc
              |> Map.put(:mix_apps, rest)
              |> Map.update!(:different_modes, &[different_mode | &1])
              |> Map.put(:last_app, rebar_app)

            {_app, _mode} ->
              case Keyword.pop(rest, rebar_app) do
                {nil, _} ->
                  missing_app = %{
                    app: rebar_app,
                    mode: rebar_mode,
                    after: last_app
                  }

                  acc
                  |> Map.update!(:missing_apps, &[missing_app | &1])
                  |> Map.put(:last_app, rebar_app)

                {^rebar_mode, rest} ->
                  different_position = %{
                    app: rebar_app,
                    mode: rebar_mode,
                    after: last_app
                  }

                  acc
                  |> Map.update!(:different_positions, &[different_position | &1])
                  |> Map.put(:last_app, rebar_app)
                  |> Map.put(:mix_apps, [{mix_app, mix_mode} | rest])

                {mode, rest} ->
                  different_mode = %{
                    app: rebar_app,
                    rebar_mode: rebar_mode,
                    mix_mode: mode
                  }

                  different_position = %{
                    app: rebar_app,
                    mode: rebar_mode,
                    after: last_app
                  }

                  acc
                  |> Map.put(:mix_apps, [{mix_app, mix_mode} | rest])
                  |> Map.update!(:different_modes, &[different_mode | &1])
                  |> Map.update!(:different_positions, &[different_position | &1])
                  |> Map.put(:last_app, rebar_app)
              end
          end
      end
    )
  end

  defp report_discrepancy(diffs, header, line_fn) do
    unless Enum.empty?(diffs) do
      IO.puts(IO.ANSI.red() <> header)

      diffs
      |> Enum.reverse()
      |> Enum.each(line_fn)

      IO.puts(IO.ANSI.reset())
    end
  end
end

CheckElixirApplications.main()
