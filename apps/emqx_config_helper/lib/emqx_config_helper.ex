defmodule EmqxConfigHelper do
	def hocon_cli(arg) do
		arg
		|> String.split()
		|> Enum.map(&to_charlist/1)
		|> :hocon_cli.main()
		:ok
	end
end
