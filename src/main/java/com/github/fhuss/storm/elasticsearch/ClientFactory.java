/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.github.fhuss.storm.elasticsearch;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * Interface to make Elasticsearch client based on the Storm map configuration.
 *
 * @author fhussonnois
 */
public interface ClientFactory<T extends Client> extends Serializable {

    public static final int DEFAULT_PORT = 9300;
    public static final String NAME  = "storm.elasticsearch.cluster.name";
    public static final String HOSTS = "storm.elasticsearch.hosts";
    public static final char PORT_SEPARATOR = ':';
    public static final char HOST_SEPARATOR = ',';

    T makeClient(Map conf) ;

    /**
     * Use this factory to create {@link TransportClient} that connects to a cluster.
     */
    public static class Transport implements ClientFactory<TransportClient> {

        private Map<String, String> settings;

        public Transport() {
        }

        public Transport(Map<String, String> settings) {
            this.settings = settings;
        }

        @Override
        public TransportClient makeClient(Map conf) {

            String clusterHosts = (String)conf.get(HOSTS);
            String clusterName  = (String)conf.get(NAME);

            Preconditions.checkNotNull(clusterHosts,"no setting found for Transport Client, make sure that you set property " + HOSTS);

            TransportClient client = TransportClient.builder().settings(buildSettings(clusterName)).build();

            for(String hostAndPort : StringUtils.split(clusterHosts, HOST_SEPARATOR)) {
                int portPos = hostAndPort.indexOf(PORT_SEPARATOR);
                boolean noPortDefined = portPos == -1;
                int port =  ( noPortDefined ) ? DEFAULT_PORT : Integer.parseInt(hostAndPort.substring(portPos + 1, hostAndPort.length()));
                String host  = (noPortDefined) ? hostAndPort : hostAndPort.substring(0, portPos);
                try {
                    client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
            }
            return client;
        }

        private Settings buildSettings(String clusterName) {
            Settings.Builder sb = Settings.settingsBuilder();
            if( StringUtils.isNotEmpty(clusterName)) sb.put("cluster.name", clusterName);
            if( settings != null) sb.put(settings);

            return sb.build();
        }
    }

    /**
     * Use this factory to create {@link TransportClient} that connects to a local cluster.
     */
    public static class LocalTransport implements ClientFactory<TransportClient> {

        private Map<String, String> settings;

        public LocalTransport() {
        }

        public LocalTransport(Map<String, String> settings) {
            this.settings = settings;
        }


        @Override
        public TransportClient makeClient(Map conf) {
            TransportClient client = TransportClient.builder().settings(buildSettings()).build();
            client.addTransportAddress(new LocalTransportAddress("1"));
            return client;
        }

        protected Settings buildSettings( ) {
            Settings.Builder sb = Settings.settingsBuilder().put("node.local", "true");
            if( settings != null) sb.put(settings);

            return sb.build();
        }
    }

    /**
     * Use this factory to create an embedded Node that acts as a node within a cluster.
     */
    public static class NodeClient implements ClientFactory<Client> {

        private Map<String, String> settings;

        public NodeClient() {}

        public NodeClient(Map<String, String> settings) {
            this.settings = settings;
        }

        @Override
        public Client makeClient(Map conf) {
            String clusterName  = (String)conf.get(NAME);

            final Node node = NodeBuilder.nodeBuilder().settings(buildSettings(clusterName)).node();
            registerShutdownHook(node);

            return node.client();
        }

        private void registerShutdownHook(final Node node) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    node.close();
                }
            });
        }

        private Settings buildSettings(String clusterName) {
            Settings.Builder sb = Settings.settingsBuilder().put("node.client", true);

            if( StringUtils.isNotEmpty(clusterName)) sb.put("cluster.name", clusterName);
            if( settings != null) sb.put(settings);

            return sb.build();
        }
    }

    /**
     * Use this factory to create a local embedded Node that acts as a node within a cluster.
     * This factory should be preferred for testing purpose.
     */
    public static class LocalNodeClient implements ClientFactory<Client> {

        private Map<String, String> settings;

        public LocalNodeClient() {}

        public LocalNodeClient(Map<String, String> settings) { this.settings = settings; }

        @Override
        public Client makeClient(Map conf) {

            final Node node = NodeBuilder.nodeBuilder().settings( buildSettings() ).node();
            registerShutdownHook(node);

            return waitForYellowStatus(node.client());
        }

        private void registerShutdownHook(final Node node) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    node.close();
                }
            });
        }

        private Client waitForYellowStatus(Client client) {
            client.admin().cluster()
                    .prepareHealth()
                    .setWaitForYellowStatus()
                    .setTimeout(TimeValue.timeValueSeconds(30))
                    .execute()
                    .actionGet();
            return client;
        }

        private Settings buildSettings( ) {
            Settings.Builder sb = Settings.settingsBuilder()
                    .put("node.name", "elastic-storm-test")
                    .put("node.local", true)
                    .put("index.store.type", "memory");

            if( settings != null) sb.put(settings);

            return sb.build();
        }
    }
}
