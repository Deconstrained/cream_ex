defmodule Cream.Supervisor.Cluster do
  require Logger
  @moduledoc false

  use Supervisor

  import Cream.Utils, only: [parse_server: 1]

  def start_link(options) do
    Supervisor.start_link(__MODULE__, options)
  end

  def init(options) do
    servers = options[:servers]

    # Make a map of server to Memcache.Connection name so that
    # Cluster.Worker process knows how to access the connections managed by
    # this supervisor.
    # Ex:
    #   %{
    #     "127.0.0.1:11211" => {:via, Registry, ...},
    #     "localhost:11211" => {:via, Registry, ...}
    #   }
    server_name_map = Enum.reduce servers, %{}, fn server, acc ->
      Map.put(acc, server, Cream.Registry.new_connection)
    end

    # Pull out the memcachex specific options.
    memcachex_options = Keyword.get(options, :memcachex, [])

    Logger.info("Initializing with options=#{inspect(options)}; memcachex_options=#{inspect(memcachex_options)}")
    # Each Memcache worker gets supervised.
    specs = Enum.map server_name_map, fn {server, name} ->
      {host, port} = parse_server(server)
      arguments = memcachex_options
        |> Keyword.merge(hostname: host, port: port)

      Logger.info("Spawning memcachex worker with arguments: #{inspect(arguments)}")
      worker(
        Memcache,
        [arguments, [name: name]],
        id: {Memcache, server}
      )
    end

    # Cluster.Worker gets supervised and passed the connection name map.
    specs = [
      worker(Cream.Cluster.Worker, [server_name_map]) | specs
    ]

    supervise(specs, strategy: :one_for_one)
  end

end
