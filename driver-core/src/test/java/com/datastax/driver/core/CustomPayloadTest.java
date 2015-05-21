/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.log4j.Logger;
import org.assertj.core.api.iterable.Extractor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.log4j.Level.TRACE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Fail.fail;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.UnsupportedFeatureException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.CassandraVersion;

import static com.datastax.driver.core.ProtocolVersion.V3;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

@CassandraVersion(major = 2.2)
public class CustomPayloadTest extends CCMBridge.PerClassSingleNodeCluster {

    private final Map<String, byte[]> customPayload = ImmutableMap.of(
        "k1", new byte[]{ 1, 2, 3 },
        "k2", new byte[]{ 4, 5, 6 }
    );

    private Logger logger = Logger.getLogger(Message.logger.getName());

    private MemoryAppender appender;

    @BeforeMethod(groups = "short")
    public void startCapturingLogs() {
        logger.addAppender(appender = new MemoryAppender());
    }

    @AfterMethod(groups = "short")
    public void stopCapturingLogs() {
        logger.setLevel(null);
        logger.removeAppender(appender);
    }

    @BeforeMethod(groups = { "short", "unit" })
    public void resetLogLevels() {
        logger.setLevel(TRACE);
    }

    // execute

    @Test(groups = "short")
    public void should_echo_custom_payload_when_executing_statement() throws Exception {
        Statement statement = new SimpleStatement("SELECT c2 FROM t1 where c1 = ?", 1);
        statement.setCustomPayload(customPayload);
        ResultSet rows = session.execute(statement);
        Map<String, byte[]> actual = rows.getExecutionInfo().getCustomPayload();
        assertCustomPayload(actual);
    }

    @Test(groups = "short")
    public void should_echo_custom_payload_when_executing_batch_statement() throws Exception {
        Statement statement = new BatchStatement().add(new SimpleStatement("INSERT INTO t1 (c1, c2) values (1, 'foo')"));
        statement.setCustomPayload(customPayload);
        ResultSet rows = session.execute(statement);
        Map<String, byte[]> actual = rows.getExecutionInfo().getCustomPayload();
        assertCustomPayload(actual);
    }

    @Test(groups = "short")
    public void should_echo_custom_payload_when_building_statement() throws Exception {
        Statement statement = QueryBuilder.select("c2").from("t1").where(eq("c1", 1)).setCustomPayload(customPayload);
        ResultSet rows = session.execute(statement);
        Map<String, byte[]> actual = rows.getExecutionInfo().getCustomPayload();
        assertCustomPayload(actual);
    }

    // prepare

    @Test(groups = "short")
    public void should_echo_custom_payload_when_preparing_statement() throws Exception {
        RegularStatement statement = new SimpleStatement("SELECT c2 FROM t1 where c1 = ?");
        statement.setCustomPayload(customPayload);
        PreparedStatement ps = session.prepare(statement);
        assertCustomPayload(ps.getCustomPayload());
    }

    // pagination

    /**
     * Ensures that a custom payload is propagated throughout pages.
     * @throws Exception
     */
    @Test(groups = "short")
    public void should_echo_custom_payload_when_paginating() throws Exception {
        session.execute("INSERT INTO t1 (c1) VALUES (1)");
        session.execute("INSERT INTO t1 (c1) VALUES (2)");
        Statement statement = new SimpleStatement("SELECT c2 FROM t1 where c1 IN (1,2)");
        statement.setFetchSize(1);
        statement.setCustomPayload(customPayload);
        ResultSet rows = session.execute(statement);
        rows.all();
        for (ExecutionInfo info : rows.getAllExecutionInfo()) {
            Map<String, byte[]> actual = info.getCustomPayload();
            assertCustomPayload(actual);
        }
    }

    // edge cases

    @Test(groups = "short", expectedExceptions = NullPointerException.class)
    public void should_throw_npe_when_null_key() throws Exception {
        Statement statement = new SimpleStatement("SELECT c2 FROM t1 where c1 = ?", 1);
        statement.setCustomPayload(ImmutableMap.<String, byte[]>of(null, new byte[]{ 1 }));
        session.execute(statement);
    }

    @Test(groups = "short", expectedExceptions = NullPointerException.class)
    public void should_throw_npe_when_null_value() throws Exception {
        Statement statement = new SimpleStatement("SELECT c2 FROM t1 where c1 = ?", 1);
        statement.setCustomPayload(ImmutableMap.<String, byte[]>of("k1", null));
        session.execute(statement);
    }

    @Test(groups = "short")
    public void should_throw_ufe_when_protocol_version_lesser_than_4() throws Exception {
        Statement statement = new SimpleStatement("SELECT c2 FROM t1 where c1 = ?", 1);
        statement.setCustomPayload(customPayload);
        Cluster otherCluster = Cluster.builder()
            .addContactPointsWithPorts(ImmutableList.of(hostAddress))
            .withProtocolVersion(V3)
            .build()
            .init();
        Session session = otherCluster.connect();
        try {
            session.execute(statement);
            fail("Should not send custom payloads with protocol V3");
        } catch (NoHostAvailableException nhae) {
            assertThat(nhae.getErrors().values())
                .extracting(new Extractor<Throwable, Throwable>() {
                    @Override
                    public Throwable extract(Throwable input) {
                        return Throwables.getRootCause(input);
                    }
                })
                .hasOnlyElementsOfType(UnsupportedFeatureException.class)
                .extracting("message")
                .containsOnly("Unsupported feature with the native protocol V3 (which is currently in use): Custom payloads are only supported since native protocol V4");
        }
    }

    // log messages

    @Test(groups = "short")
    public void should_print_log_message() throws Exception {
        Statement statement = new SimpleStatement("SELECT c2 FROM t1 where c1 = ?", 1);
        statement.setCustomPayload(customPayload);
        session.execute(statement);
        String logs = appender.waitAndGet(10000);
        assertThat(logs)
            .contains("Sending payload: {k1:0x010203, k2:0x040506} (20 bytes total)")
            .contains("Received payload: {k1:0x010203, k2:0x040506} (20 bytes total)");
    }

    @Override
    protected String getJvmArgs() {
        return "-Dcassandra.custom_query_handler_class=org.apache.cassandra.cql3.CustomPayloadMirroringQueryHandler";
    }

    @Override
    protected Collection<String> getTableDefinitions() {
        return Collections.singletonList("CREATE TABLE t1 (c1 int PRIMARY KEY, c2 text)");
    }

    private void assertCustomPayload(Map<String, byte[]> actual) {
        assertThat(actual).containsOnly(
            entry("k1", new byte[]{ 1, 2, 3 }),
            entry("k2", new byte[]{ 4, 5, 6 })
        );
    }

}
