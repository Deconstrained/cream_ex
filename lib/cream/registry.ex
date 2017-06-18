defmodule Cream.Registry do

  def new_cluster do
    {:via, Registry, {Cream.Registry, {Cream.Cluster, UUID.uuid4}}}
  end

  def new_connection do
    {:via, Registry, {Cream.Registry, {Memcache.Connection, UUID.uuid4}}}
  end

end
