defmodule Cream.Supervisor.Cluster do
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
    # Note: namespace is prepended in this library to make it possible to make
    # the load balancing algorithm equivalent to Dalli, so we need to disable
    # sending the namespace argument to Memcachex lest it be prepended more
    # than once
    memcachex_namespace = Keyword.get(memcachex_options, :namespace, nil)
    namespace = Map.get(options, :namespace, memcachex_namespace)
    # Extra Dalli compatibility option for dealing with super long keys
    digest_class = Map.get(options, :digest_class, :md5)

    # Each Memcache worker gets supervised.
    specs = Enum.map server_name_map, fn {server, name} ->
      {host, port} = parse_server(server)
      arguments = memcachex_options
        |> Keyword.merge(hostname: host, port: port)
        |> Keyword.delete(:namespace)

      worker(
        Memcache,
        [arguments, [name: name]],
        id: {Memcache, server}
      )
    end

    # Cluster.Worker gets supervised and passed the connection name map.
    specs = [
      worker(Cream.Cluster.Worker, [%{connection_map: server_name_map, namespace: namespace, digest_class: digest_class}]) | specs
    ]

    supervise(specs, strategy: :one_for_one)
  end

end
