#!/usr/bin/env elixir

defmodule FiatLux do
  def main(argv) do
    {flags, [app_path]} = OptionParser.parse!(argv, strict: [force: :boolean])
    force = Keyword.get(flags, :force, false)
    ["apps", app_name] = Path.split(app_path)

    {:ok, rebar_props} =
      app_path
      |> Path.join("rebar.config")
      |> :file.consult()

    {:ok, [{:application, app_name, app_props}]} =
      [app_path, "src", app_name <> ".app.src"]
      |> Path.join()
      |> :file.consult()

    app_deps =
      app_props
      |> Keyword.get(:applications, [])
      |> Kernel.--([:kernel, :stdlib])

    umbrella_deps =
      rebar_props
      |> Keyword.get(:deps, [])
      # |> Enum.filter(fn x ->
      #   match?({_, {:path, _}}, x)
      # end)
      |> Enum.map(fn
        {app, {:path, _}} ->
          # hack to format 2-tuple
          {:__block__, [], [{app, in_umbrella: true}]}

        {app, {:git, url, {:tag, tag}}} ->
            slug =
              url
              |> to_string()
              |> String.replace_prefix("https://github.com/", "")
              |> String.replace_suffix(".git", "")
          # hack to format 2-tuple
          {:__block__, [], [{app, github: slug, tag: to_string(tag)}]}

        {app, {:git_subdir, url, {:tag, tag}, subdir}} ->
            slug =
              url
              |> to_string()
              |> String.replace_prefix("https://github.com/", "")
              |> String.replace_suffix(".git", "")
          # hack to format 2-tuple
          {:__block__, [], [{app, github: slug, tag: to_string(tag), sparse: to_string(subdir)}]}

        {app, vsn} when is_list(vsn) ->
          # hack to format 2-tuple
          {:__block__, [], [{app, to_string(vsn)}]}
      end)

    app_mod = Keyword.get(app_props, :mod)

    code_str =
      mix_project(app_name, app_mod, umbrella_deps)
      |> Code.quoted_to_algebra()
      |> Inspect.Algebra.format(95)

    IO.puts(code_str)

    dest = Path.join([app_path, "mix.exs"])
    if File.exists?(dest) && not force do
      IO.puts("#{dest} already exists!")
      exit(1)
    else
      File.write!(dest, [code_str, "\n"])
      IO.puts("written #{dest}")
    end
  end

  def mix_project(app_name, app_mod, umbrella_deps) do
    application = if app_mod do
      quote do
        [
          extra_applications: UMP.extra_applications(),
          mod: unquote(app_mod)
        ]
      end
    else
      quote do
        [
          extra_applications: UMP.extra_applications()
        ]
      end
    end
    mod_name =
      app_name
      |> to_string()
      |> String.replace_prefix("emqx_", "")
      |> Macro.camelize()
      |> then(& "Elixir.EMQX" <> &1 <> ".MixProject")
      |> String.to_atom()
    quote do
      defmodule unquote(mod_name) do
        use Mix.Project
        alias EMQXUmbrella.MixProject, as: UMP

        def project do
          [
            app: unquote(app_name),
            version: "0.1.0",
            build_path: "../../_build",
            # config_path: "../../config/config.exs",
            erlc_options: UMP.erlc_options(),
            erlc_paths: UMP.erlc_paths(),
            deps_path: "../../deps",
            lockfile: "../../mix.lock",
            elixir: "~> 1.14",
            start_permanent: Mix.env() == :prod,
            deps: deps()
          ]
        end

        # Run "mix help compile.app" to learn about applications
        def application do
          unquote(application)
        end

        def deps() do
          [
            unquote_splicing(umbrella_deps)
          ]
        end
      end
    end
  end
end

System.argv()
|> FiatLux.main()
