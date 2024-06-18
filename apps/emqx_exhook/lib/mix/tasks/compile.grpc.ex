defmodule Mix.Tasks.Compile.Grpc do
  use Mix.Task.Compiler

  @recursive true
  # TODO: use manifest to track generated files?

  @impl true
  def run(_args) do
    Mix.Project.get!()
    config = Mix.Project.config()
    %{
        gpb_opts: gpb_opts,
        proto_dirs: proto_dirs,
        out_dir: out_dir
    } = config[:grpc_opts]

    app_root = File.cwd!()
    app_build_path = Mix.Project.app_path(config)

    proto_srcs =
      proto_dirs
      |> Enum.map(& Path.join([app_root, &1]))
      |> Mix.Utils.extract_files([:proto])

    Enum.each(proto_srcs, & compile_pb(&1, app_root, app_build_path, out_dir, gpb_opts))

    {:noop, []}
  end

  defp compile_pb(proto_src, app_root, app_build_path, out_dir, gpb_opts) do
    ebin_path = Path.join([app_build_path, "ebin"])
    basename = proto_src |> Path.basename(".proto") |> to_charlist()
    prefix = Keyword.get(gpb_opts, :module_name_prefix, '')
    suffix = Keyword.get(gpb_opts, :module_name_suffix, '')
    mod_name = '#{prefix}#{basename}#{suffix}'
    opts = [
      :use_packages,
      :maps,
      :strings_as_binaries,
      i: '.',
      o: out_dir,
      report_errors: false,
      rename: {:msg_name, :snake_case},
      rename: {:msg_fqname, :base_name},
    ]
    File.mkdir_p!(out_dir)
    # TODO: better error logging...
    :ok = :gpb_compile.file(
      to_charlist(proto_src),
      opts ++ gpb_opts
    )
    generated_src = Path.join([app_root, out_dir, "#{mod_name}.erl"])
    |> IO.inspect(label: :generated_src)
    generated_ebin = Path.join([ebin_path, "#{mod_name}.beam"])
    |> IO.inspect(label: :generated_ebin)
    gpb_include_dir = :code.lib_dir(:gpb, :include)

    compile_res = :compile.file(
      to_charlist(generated_src),
      [
        :return_errors,
        i: to_charlist(gpb_include_dir),
        outdir: to_charlist(ebin_path)
      ]
    )
    # todo: error handling & logging
    case compile_res do
      {:ok, _} ->
        :ok

      {:ok, _, _warnings} ->
        :ok
    end

    mod_name
    |> List.to_atom()
    |> :code.purge()

    {:module, mod} =
      ebin_path
      |> Path.join(mod_name)
      |> to_charlist()
      |> :code.load_abs()

    mod_name = List.to_atom(mod_name)
    service_quoted = EEx.compile_file("lib/emqx/grpc/template/service.eex")
    client_quoted = EEx.compile_file("lib/emqx/grpc/template/client.eex")

    mod_name.get_service_names()
    |> Enum.each(fn service ->
      service
      |> mod_name.get_service_def()
      |> then(fn {{:service, service_name}, methods} ->
        methods = Enum.map(methods, fn method ->
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
        {result, _bindings} = Code.eval_quoted(
          service_quoted,
          methods: methods,
          module_name: snake_service,
          unmodified_service_name: service_name)
        result = String.replace(result, ~r/\n\n\n+/, "\n\n\n")
        output_src = Path.join([app_root, out_dir, "#{snake_service}_bhvr.erl"])
        File.write!(output_src, result)

        {result, _bindings} = Code.eval_quoted(
          client_quoted,
          methods: methods,
          pb_module: mod_name,
          module_name: snake_service,
          unmodified_service_name: service_name)
        result = String.replace(result, ~r/\n\n\n+/, "\n\n\n")
        output_src = Path.join([app_root, out_dir, "#{snake_service}_client.erl"])
        File.write!(output_src, result)

        {{:service, service_name}, methods}
      end)
    end)

    mod_name
  end
end
