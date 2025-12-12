defmodule Mix.Tasks.Emqx.Depxref do
  use Mix.Task

  @shortdoc "Run depxref analysis"
  @requirements ["compile", "loadpaths"]

  @xref :depxref
  @xref_ignore_apps [:emqx_mix_utils]
  @xref_common_libs [:erts, :stdlib, :kernel, :elixir]
  @xref_ignore_missing [:snabbkaffe]

  defmodule Report do
    alias Mix.Tasks.Emqx.Depxref

    def apps(apps, iodev \\ :stdio) when is_list(apps) do
      Enum.map(apps, fn app ->
        {:ok, strbuf} = StringIO.open("")
        issues? = app(app, strbuf)
        {:ok, {_, report}} = StringIO.close(strbuf)

        if issues? do
          IO.puts(iodev, "<details><summary>#{app}</summary>")
          IO.puts(iodev, "")
          IO.puts(iodev, report)
          IO.puts(iodev, "")
          IO.puts(iodev, "</details>")
          IO.puts(iodev, "")
        end

        issues?
      end)
      |> Enum.any?()
    end

    def app(app, iodev \\ :stdio) do
      undeclared = Depxref.undeclared_dependencies(app)
      unneeded = Depxref.unneeded_dependencies(app)
      cyclic = Depxref.cyclic_dependencies(app)

      if undeclared != [] or unneeded != [] or cyclic != [] do
        report_issues(app, undeclared, unneeded, cyclic, iodev)
        true
      else
        false
      end
    end

    defp report_issues(app, undeclared, unneeded, cyclic, iodev) do
      if undeclared != [] do
        IO.puts(iodev, "### Application `#{app}` uses undeclared direct dependencies")

        Enum.each(undeclared, fn {dep, callsites} ->
          if List.keyfind(cyclic, dep, 0) != nil do
            IO.puts(iodev, " * `#{dep}` (cyclic)")
          else
            IO.puts(iodev, " * `#{dep}`")

            Enum.each(callsites, fn {from, to} ->
              IO.puts(
                iodev,
                "    - #{as_vtype(from)} `#{as_vertex(from)}` calls `#{as_vertex(to)}`"
              )
            end)
          end
        end)

        IO.puts(iodev, "")
      end

      if unneeded != [] do
        IO.puts(iodev, "### Application `#{app}` declares seemingly unneeded dependencies")
        Enum.each(unneeded, fn dep -> IO.puts(iodev, " * `#{dep}`") end)
        IO.puts(iodev, "")
      end

      if cyclic != [] do
        IO.puts(iodev, "### Application `#{app}` has cyclic dependencies")

        Enum.each(cyclic, fn {dep, callsites, backsites} ->
          IO.puts(iodev, " * `#{dep}`")

          if List.keyfind(undeclared, dep, 0) != nil do
            Enum.each(callsites, fn {from, to} ->
              IO.puts(
                iodev,
                "    - #{as_vtype(from)} `#{as_vertex(from)}` calls `#{as_vertex(to)}`"
              )
            end)
          else
            Enum.each(backsites, fn {from, to} ->
              IO.puts(
                iodev,
                "    - #{as_vtype(to)} `#{as_vertex(to)}` is called by `#{as_vertex(from)}`"
              )
            end)
          end
        end)

        IO.puts(iodev, "")
      end
    end

    # defp as_edge({from, to}) do
    #   "#{as_vertex(from)} -> #{as_vertex(to)}"
    # end

    defp as_vtype({_mod, _func, _arity}) do
      "function"
    end

    defp as_vtype(_mod) do
      "module"
    end

    defp as_vertex({mod, func, arity}) do
      "#{mod}:#{func}/#{arity}"
    end

    defp as_vertex(mod) do
      "#{mod}"
    end
  end

  @impl true
  def run(args) do
    {:ok, app_names} = initialize()

    {opts, rest} =
      OptionParser.parse!(
        args,
        strict: [
          output: :string
        ]
      )

    iodev =
      case opts[:output] do
        nil ->
          :stdio

        "-" ->
          :stdio

        filename ->
          File.open!(filename, [:write])
      end

    apps_report =
      case rest do
        [] ->
          app_names

        names ->
          apps_in = names |> Enum.map(&String.to_atom/1)
          apps_in = apps_in -- @xref_ignore_apps
          unknown = apps_in -- app_names

          if unknown != [] do
            Mix.raise("Unknown applications: #{inspect(unknown)}")
          end

          apps_in
      end

    issues? = Report.apps(apps_report, iodev)

    if issues? do
      System.halt(1)
    else
      :ok
    end
  end

  def unneeded_dependencies(app) do
    with %Mix.Dep{opts: opts} <- find_app_info(app) do
      {:ok, dependencies} = query("range (strict AE | #{app})")
      declared = opts[:app_properties][:applications] || []
      unneeded = declared -- dependencies
      unneeded = unneeded -- @xref_common_libs
      unneeded -- @xref_ignore_missing
    end
  end

  def undeclared_dependencies(app) do
    with %Mix.Dep{opts: opts} <- find_app_info(app) do
      {:ok, dependencies} = query("range (strict AE | #{app})")
      declared = opts[:app_properties][:applications] || []
      dependencies = dependencies -- @xref_common_libs
      undeclared = dependencies -- declared

      Enum.map(undeclared, fn dep ->
        {:ok, callsites} = query("ME * (App) [#{app} -> #{dep}]")
        {dep, callsites}
      end)
    end
  end

  def cyclic_dependencies(app) do
    case query("range (strict AE | #{app} || EMQX)") do
      {:ok, dependencies = [_ | _]} ->
        {:ok, backdeps} = query("domain (strict AE | #{as_set(dependencies)} || #{app})")

        Enum.map(backdeps, fn dep ->
          {:ok, callsites} = query("ME * (App) [#{app} -> #{dep}]")
          {:ok, backsites} = query("ME * (App) [#{dep} -> #{app}]")
          {dep, callsites, backsites}
        end)

      {:ok, []} ->
        []
    end
  end

  def query(query) do
    :xref.q(@xref, to_charlist(query))
  end

  def apps() do
    Mix.Dep.Umbrella.cached()
    |> Stream.reject(fn app -> app.app in @xref_ignore_apps end)
    |> Enum.map(fn app -> app.app end)
  end

  def initialize() do
    :ok = ensure_xref()

    case query("EMQX") do
      {:ok, [_ | _]} = ok ->
        ok

      _undefined ->
        rebuild_xref()
    end
  end

  defp rebuild_xref() do
    apps = Mix.Dep.Umbrella.cached()
    apps = apps |> Stream.reject(fn app -> app.app in @xref_ignore_apps end)
    deps = Mix.Dep.cached()
    deps = deps |> Stream.reject(fn dep -> dep.app in @xref_ignore_apps end)

    app_names = apps |> Enum.map(fn app -> app.app end)
    dep_names = deps |> Enum.map(fn dep -> dep.app end)

    rel_otp = :code.lib_dir()

    dir_apps =
      apps
      |> Stream.map(fn app -> to_charlist(app.opts[:build]) end)

    dir_deps =
      deps
      |> Stream.reject(fn app -> app.app in app_names end)
      |> Stream.map(fn dep -> to_charlist(dep.opts[:build]) end)

    # Add Erlang/OTP applications as release:
    {:ok, _} = :xref.add_release(@xref, rel_otp, name: :otp)

    # Add applications and dependencies:
    Enum.each(dir_deps, fn lib -> {:ok, _} = :xref.add_application(@xref, lib) end)
    Enum.each(dir_apps, fn lib -> {:ok, _} = :xref.add_application(@xref, lib) end)

    # Setup shortcut variables:
    # * `EMQX` is the applications in the umbrella project
    # * `DEPS` is the dependencies for those applications
    {:ok, _} = query("EMQX := (App) #{as_set(app_names)}")
    {:ok, _} = query("DEP := (App) #{as_set(dep_names)}")
    {:ok, app_names}
  end

  defp find_app_info(app) do
    apps = Mix.Dep.Umbrella.cached()
    Enum.find(apps, fn dep -> dep.app == app end)
  end

  defp as_set(atoms) do
    "[#{atoms |> Enum.map(&to_string/1) |> Enum.join(", ")}]"
  end

  def ensure_xref() do
    case :xref.start(@xref) do
      {:ok, _} ->
        :ok

      {:error, {:already_started, _}} ->
        :xref.stop(@xref)
        ensure_xref()
    end
  end

  def stop_xref() do
    :xref.stop(@xref)
  end
end
