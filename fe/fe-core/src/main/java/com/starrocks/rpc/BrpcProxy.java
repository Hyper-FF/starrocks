// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.rpc;

import com.baidu.jprotobuf.pbrpc.client.ProtobufRpcProxy;
import com.baidu.jprotobuf.pbrpc.transport.AbstractChannelPoolSharableFactory;
import com.baidu.jprotobuf.pbrpc.transport.RpcChannel;
import com.baidu.jprotobuf.pbrpc.transport.RpcClient;
import com.baidu.jprotobuf.pbrpc.transport.RpcClientOptions;
import com.starrocks.common.Config;
import com.starrocks.common.util.DnsCache;
import com.starrocks.service.FrontendOptions;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BrpcProxy {
    private static final Logger LOG = LogManager.getLogger(BrpcProxy.class);

    private final RpcClient rpcClient;
    private final ConcurrentHashMap<TNetworkAddress, PBackendService> backendServiceMap;
    private final ConcurrentHashMap<TNetworkAddress, LakeService> lakeServiceMap;
    private final EvictableChannelPoolFactory channelPoolFactory = new EvictableChannelPoolFactory();

    public BrpcProxy() {
        final RpcClientOptions rpcOptions = new RpcClientOptions();
        // If false, different methods to a service endpoint use different connection pool,
        // which will create too many connections.
        // If true, all the methods to a service endpoint use the same connection pool.
        rpcOptions.setShareThreadPoolUnderEachProxy(true);
        rpcOptions.setShareChannelPool(true);
        rpcOptions.setMaxTotoal(Config.brpc_connection_pool_size);
        // After the RPC request sending, the connection will be closed,
        // if the number of total connections is greater than MaxIdleSize.
        // Therefore, MaxIdleSize shouldn't less than MaxTotal for the async requests.
        rpcOptions.setMaxIdleSize(Config.brpc_connection_pool_size);
        rpcOptions.setMaxWait(Config.brpc_idle_wait_max_time);
        rpcOptions.setJmxEnabled(true);
        rpcOptions.setReuseAddress(Config.brpc_reuse_addr);
        rpcOptions.setMinEvictableIdleTime(Config.brpc_min_evictable_idle_time_ms);
        rpcOptions.setShortConnection(Config.brpc_short_connection);
        rpcOptions.setInnerResuePool(Config.brpc_inner_reuse_pool);

        rpcClient = new RpcClient(rpcOptions);
        backendServiceMap = new ConcurrentHashMap<>();
        lakeServiceMap = new ConcurrentHashMap<>();
    }

    private static BrpcProxy getInstance() {
        return BrpcProxy.SingletonHolder.INSTANCE;
    }

    /**
     * Only used for pseudo cluster or unittest
     */
    public static void setInstance(BrpcProxy proxy) {
        BrpcProxy.SingletonHolder.INSTANCE = proxy;
    }

    public static TNetworkAddress convertToIpAddress(TNetworkAddress address) {
        if (!FrontendOptions.isUseFqdn()) {
            return address;
        }
        String ip = DnsCache.tryLookup(address.getHostname());
        return new TNetworkAddress(ip, address.getPort());
    }

    public static PBackendService getBackendService(TNetworkAddress address) {
        return getInstance().getBackendServiceImpl(address);
    }

    public static LakeService getLakeService(TNetworkAddress address) throws RpcException {
        return getInstance().getLakeServiceImpl(address);
    }

    public static LakeService getLakeService(String host, int port) throws RpcException {
        return getInstance().getLakeServiceImpl(new TNetworkAddress(host, port));
    }

    protected PBackendService getBackendServiceImpl(TNetworkAddress address) {
        TNetworkAddress cacheAddress = convertToIpAddress(address);
        return backendServiceMap.computeIfAbsent(cacheAddress, this::createBackendService);
    }

    protected LakeService getLakeServiceImpl(TNetworkAddress address) throws RpcException {
        try {
            TNetworkAddress cacheAddress = convertToIpAddress(address);
            return lakeServiceMap.computeIfAbsent(cacheAddress, this::createLakeService);
        } catch (Exception e) {
            throw new RpcException("fail to initialize the LakeService on node " + address.getHostname(), e);
        }
    }

    /**
     * Throw away the cached stubs and the pooled connections for one endpoint, so that the next RPC
     * to it starts from a fresh connection.
     * <p>
     * Call this once the caller has concluded that the endpoint is unreachable, e.g. right after it
     * goes on the host blacklist. The connections we hold are then either already dead or - the case
     * this exists for - still ESTABLISHED but no longer able to carry traffic, which nothing else in
     * the stack detects. See {@link EvictableChannelPoolFactory} for why the pool cannot clean up
     * after itself.
     */
    public static void invalidateEndpoint(TNetworkAddress address) {
        getInstance().invalidateEndpointImpl(address);
    }

    protected void invalidateEndpointImpl(TNetworkAddress address) {
        TNetworkAddress cacheAddress = convertToIpAddress(address);
        // The stubs have to go as well, not just the pool entry: ProtobufRpcProxy resolves its
        // RpcChannel once, when proxy() is called, and holds on to that reference for its whole
        // life. A stub kept in the map would keep talking to the channel we are dropping here.
        backendServiceMap.remove(cacheAddress);
        lakeServiceMap.remove(cacheAddress);
        channelPoolFactory.evict(cacheAddress.getHostname(), cacheAddress.getPort());
        LOG.info("dropped cached brpc connections to {}:{}", cacheAddress.getHostname(), cacheAddress.getPort());
    }

    private PBackendService createBackendService(TNetworkAddress address) {
        ProtobufRpcProxy<PBackendService> proxy = new ProtobufRpcProxy<>(rpcClient, PBackendService.class);
        proxy.setChannelPoolSharableFactory(channelPoolFactory);
        proxy.setHost(address.getHostname());
        proxy.setPort(address.getPort());
        return new PBackendServiceWithMetrics(proxy.proxy());
    }

    private LakeService createLakeService(TNetworkAddress address) {
        ProtobufRpcProxy<LakeService> proxy = new ProtobufRpcProxy<>(rpcClient, LakeService.class);
        proxy.setChannelPoolSharableFactory(channelPoolFactory);
        proxy.setHost(address.getHostname());
        proxy.setPort(address.getPort());
        return new LakeServiceWithMetrics(proxy.proxy());
    }

    /**
     * Owns the endpoint -> connection pool mapping so that a pool can be taken out of service.
     * <p>
     * jprotobuf never retires a pooled connection on its own. {@code ChannelPool} configures the
     * commons-pool2 pool without {@code timeBetweenEvictionRuns}, so the evictor thread never starts
     * and {@code minEvictableIdleTime} is dead configuration; {@code validateObject()} only asks
     * {@code isOpen() && isActive()}, which a socket that is ESTABLISHED but no longer being read or
     * written still answers true to; and a call that times out never invalidates the connection it
     * used. A connection that has stopped carrying traffic therefore stays in the pool for the life
     * of the process and silently swallows its share of every later RPC to that endpoint.
     * <p>
     * The default factory also keeps this mapping in a static {@link java.util.HashMap} that
     * {@code getOrCreateChannelPool} writes without synchronization, so the override below uses a
     * concurrent map and an atomic get-or-create.
     */
    private static class EvictableChannelPoolFactory extends AbstractChannelPoolSharableFactory {
        private final Map<String, RpcChannel> rpcChannelMap = new ConcurrentHashMap<>();

        @Override
        protected Map<String, RpcChannel> getRpcChannelMap() {
            return rpcChannelMap;
        }

        @Override
        public RpcChannel getOrCreateChannelPool(RpcClient rpcClient, String host, int port) {
            return rpcChannelMap.computeIfAbsent(getHostAddress(host, port),
                    key -> new RpcChannel(rpcClient, host, port));
        }

        private void evict(String host, int port) {
            RpcChannel channel = rpcChannelMap.remove(getHostAddress(host, port));
            if (channel != null) {
                // Closes the pool and with it every connection in it. Calls that are still in flight
                // on a borrowed connection fail fast, which beats waiting out the RPC timeout on a
                // connection we have already decided is unusable.
                channel.close();
            }
        }
    }

    private static class SingletonHolder {
        private static BrpcProxy INSTANCE = new BrpcProxy();
    }
}
