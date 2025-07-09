defmodule Mix.Tasks.Compile.Grpc do
  use Mix.Task.Compiler

  alias EMQXUmbrella.MixProject, as: UMP

  @recursive true
  @manifest_vsn 1
  @manifest "compile.grpc"
  # TODO: use manifest to track generated files?

  @stale? {__MODULE__, :stale?}

  @impl true
  def manifests(), do: [manifest()]
  defp manifest(), do: Path.join(Mix.Project.manifest_path(), @manifest)

  @impl true
  def run(_args) do
    Mix.Project.get!()
    config = Mix.Project.config()

    %{
      gpb_opts: gpb_opts,
      proto_dirs: proto_dirs,
      out_dir: out_dir
    } = config[:grpc_opts]

    add_to_path_and_cache(:syntax_tools)
    :ok = Application.ensure_loaded(:syntax_tools)
    :ok = Application.ensure_loaded(:gpb)

    app_root = File.cwd!()
    app_build_path = Mix.Project.app_path(config)

    proto_srcs =
      proto_dirs
      |> Enum.map(&Path.join([app_root, &1]))
      |> Mix.Utils.extract_files([:proto])

    manifest_data = read_manifest(manifest())

    context = %{
      manifest_data: manifest_data,
      app_root: app_root,
      app_build_path: app_build_path,
      out_dir: out_dir,
      gpb_opts: gpb_opts
    }

    Enum.each(proto_srcs, &compile_pb(&1, context))

    if Process.get(@stale?, false) do
      write_manifest(manifest(), manifest_data)
    end

    :noop
  after
    Process.delete(@stale?)
    Application.unload(:gpb)
    Application.unload(:syntax_tools)
  end

  defp compile_pb(proto_src, context) do
    %{
      app_root: app_root,
      app_build_path: app_build_path,
      out_dir: out_dir,
      gpb_opts: gpb_opts
    } = context

    manifest_modified_time = Mix.Utils.last_modified(manifest())
    ebin_path = Path.join([app_build_path, "ebin"])
    basename = proto_src |> Path.basename(".proto") |> to_charlist()
    prefix = Keyword.get(gpb_opts, :module_name_prefix, ~c"")
    suffix = Keyword.get(gpb_opts, :module_name_suffix, ~c"")
    mod_name = ~c"#{prefix}#{basename}#{suffix}"

    opts = [
      :use_packages,
      :maps,
      :strings_as_binaries,
      i: ~c".",
      o: out_dir,
      report_errors: false,
      rename: {:msg_name, :snake_case},
      rename: {:msg_fqname, :base_name}
    ]

    if stale?(proto_src, manifest_modified_time) do
      Process.put(@stale?, true)
      debug("compiling proto file: #{proto_src}")
      File.mkdir_p!(out_dir)
      # TODO: better error logging...
      :ok =
        :gpb_compile.file(
          to_charlist(proto_src),
          opts ++ gpb_opts
        )
    else
      debug("proto file up to date, not compiling: #{proto_src}")
    end

    generated_src = Path.join([app_root, out_dir, "#{mod_name}.erl"])
    gpb_include_dir = :code.lib_dir(:gpb) |> Path.join("include")

    if stale?(generated_src, manifest_modified_time) do
      Process.put(@stale?, true)
      debug("compiling proto module: #{generated_src}")

      compile_res =
        :compile.file(
          to_charlist(generated_src),
          [
            :return_errors,
            i: to_charlist(gpb_include_dir),
            outdir: to_charlist(ebin_path)
          ] ++ UMP.erlc_options()
        )

      # todo: error handling & logging
      case compile_res do
        {:ok, _} ->
          :ok

        {:ok, _, _warnings} ->
          :ok
      end
    else
      debug("file up to date, not compiling: #{generated_src}")
    end

    mod_name
    |> List.to_atom()
    |> :code.purge()

    {:module, _mod} =
      ebin_path
      |> Path.join(mod_name)
      |> to_charlist()
      |> :code.load_abs()

    mod_name = List.to_atom(mod_name)

    service_quoted =
      [__DIR__, "../../", "emqx/grpc/template/service.eex"]
      |> Path.join()
      |> Path.expand()
      |> EEx.compile_file()

    client_quoted =
      [__DIR__, "../../", "emqx/grpc/template/client.eex"]
      |> Path.join()
      |> Path.expand()
      |> EEx.compile_file()

    mod_name.get_service_names()
    |> Enum.each(fn service ->
      service
      |> mod_name.get_service_def()
      |> then(fn {{:service, service_name}, methods} ->
        methods =
          Enum.map(methods, fn method ->
            snake_case = method.name |> to_string() |> Macro.underscore()
            message_type = mod_name.msg_name_to_fqbin(method.input)

            method
            |> Map.put(:message_type, message_type)
            |> Map.put(:snake_case, snake_case)
            |> Map.put(:pb_module, mod_name)
            |> Map.put(:unmodified_method, method.name)
          end)

        snake_service =
          service_name
          |> to_string()
          |> Macro.underscore()
          |> String.replace("/", "_")
          |> String.replace(~r/(.)([0-9]+)/, "\\1_\\2")

        bindings = [
          methods: methods,
          pb_module: mod_name,
          module_name: snake_service,
          unmodified_service_name: service_name
        ]

        bhvr_output_src = Path.join([app_root, out_dir, "#{snake_service}_bhvr.erl"])

        if stale?(bhvr_output_src, manifest_modified_time) do
          Process.put(@stale?, true)
          render_and_write(service_quoted, bhvr_output_src, bindings)
        else
          debug("file up to date, not compiling: #{bhvr_output_src}")
        end

        client_output_src = Path.join([app_root, out_dir, "#{snake_service}_client.erl"])

        if stale?(client_output_src, manifest_modified_time) do
          Process.put(@stale?, true)
          render_and_write(client_quoted, client_output_src, bindings)
        else
          debug("file up to date, not compiling: #{client_output_src}")
        end

        :ok
      end)
    end)

    :ok
  end

  defp stale?(file, manifest_modified_time) do
    with true <- File.exists?(file),
         false <- Mix.Utils.stale?([file], [manifest_modified_time]) do
      false
    else
      _ -> true
    end
  end

  defp read_manifest(file) do
    try do
      file |> File.read!() |> :erlang.binary_to_term()
    rescue
      _ -> %{}
    else
      {@manifest_vsn, data} when is_map(data) -> data
      _ -> %{}
    end
  end

  defp write_manifest(file, data) do
    debug("writing manifest #{file}")
    File.mkdir_p!(Path.dirname(file))
    File.write!(file, :erlang.term_to_binary({@manifest_vsn, data}))
  end

  defp render_and_write(quoted_file, output_src, bindings) do
    {result, _bindings} = Code.eval_quoted(quoted_file, bindings)
    result = String.replace(result, ~r/\n\n\n+/, "\n\n\n")
    File.write!(output_src, result)
  end

  def add_to_path_and_cache(lib_name) do
    :code.lib_dir()
    |> Path.join("#{lib_name}-*")
    |> Path.wildcard()
    |> hd()
    |> Path.join("ebin")
    |> to_charlist()
    |> :code.add_path(:cache)
  end

  defp debug(iodata) do
    if Mix.debug?() do
      Mix.shell().info(IO.ANSI.format([:cyan, iodata]))
    end
  end
end
