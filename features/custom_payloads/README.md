## Custom Payloads

The native protocol version 4 introduces a new feature called [Custom Payloads][CASSANDRA-8553].

According to the [protocol V4 specification][v4spec], custom payloads are generic key-value maps
where keys are strings and each value is an arbitrary sequence of bytes.

Custom payloads can be used to convey user-specific information from clients to servers and vice versa.

[CASSANDRA-8553]: https://issues.apache.org/jira/browse/CASSANDRA-8553
[v4spec]: https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec

Currently payloads can be sent by clients along with all `QUERY`, `PREPARE`, `EXECUTE` and `BATCH` requests.

### Enabling custom payloads on C* nodes

What the server is supposed to do with a payload depends on the [QueryHandler][qh] used server-side
to decode requests. Unsurprisingly, the default `QueryHandler` implementation used by Cassandra 
simply ignores all payloads. However, users can replace it with their own implementation,
thus being able to to process user-specific payloads; the payload format as well as
the serialization and deserialization logic is entirely left for clients to implement.
Needless to say, the same encoding and decoding logic should be applied on both client
and server endpoints.

[qh]: https://issues.apache.org/jira/browse/CASSANDRA-6659

This section is a small how-to to help driver users getting started with 
custom payload processing server-side. For more detailed information, 
please refer to the Cassandra documentation itself.

To enable a custom `QueryHandler` for a Cassandra node, its JVM should be
started with the system property `cassandra.custom_query_handler_class` set
to a fully qualified name of a class implementing `org.apache.cassandra.cql3.QueryHandler`.
This class should be of course available on the node's classpath.

This can be achieved with the following steps:

1. Add your implementation jar to the `CLASSPATH` environment variable;
2. Add the following to `$CASSANDRA_CONF/cassandra-env.sh`:

```
JVM_EXTRA_OPTS="-Dcassandra.custom_query_handler_class=fully.qualified.name.of.MyQueryHandler"
```

Of course, all nodes in the cluster *must* be started with the same QueryHandler, otherwise
payloads sent by the driver could get lost.

### Implementation Notes

Payloads in the Java driver are represented as `Map<String,byte[]>` instances;
note that `null` is forbidden as a key and as a value; including a `null` in your
payload will result in a `NullPointerException` (wrapped in a [NoHostAvailableException][nhae])
being thrown when the request is encoded.

*Users should always use thread-safe implementations* when creating payloads; using unsynchronized implementations 
such as `java.util.HashMap` will lead to concurrency problems with unexpected results. Good implementation choices
are: [ConcurrentHashMap][chm], or Guava's [ImmutableMap][immutablemap]. On the same note, modifying the payload *after* 
submitting it to the driver could lead to unexpected results.

There is currently no hard-coded limitation for the length of a custom payload, neither in terms
of number of entries, nor in terms of total payload length; however *users are strongly encouraged 
to avoid sending large payloads*.

[nhae]: http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/exceptions/NoHostAvailableException.html
[chm]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ConcurrentHashMap.html
[immutablemap]: http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/collect/ImmutableMap.html

### Sending custom payloads

A custom payload can be attached to any `Statement` via `Statement.setCustomPayload()`.

Once the payload is set, you can either prepare the statement or execute it; 
in both cases, the payload will be sent along with the `PREPARE` or `QUERY` request respectively:

```java
Statement statement = new SimpleStatement("SELECT foo FROM bar WHERE qix = 1");
statement.setCustomPayload(customPayload);
session.execute(statement); //payload will be sent with QUERY request
session.prepare(statement); //payload will be sent with PREPARE request
```

Note that if you prepare a statement containing a payload, the payload will be sent 
when the request is prepared only; *it will not be sent again when executing a `BoundStatement` obtained from it*. 
If you want to send payloads along with bound statements, do the following instead:

```java
Statement statement = new SimpleStatement("SELECT foo FROM bar WHERE qix = ?");
PreparedStatement ps = session.prepare(statement); //no payload is sent with the PREPARE request
BoundStatement bs = ps.bind(1);
bs.setCustomPayload(customPayload);
session.execute(statement); //payload will be sent with EXECUTE request
```

Naturally, you can also set a payload when using the `QueryBuilder` API:

```java
Statement statement = QueryBuilder.select("c2").from("t1").where(eq("c1", 1)).setCustomPayload(customPayload);
session.execute(statement); //payload will be sent with EXECUTE request
```

When sending payloads with batches, please note that payloads must be attached to the 
`BatchStatement` itself, *not* to internal statements:

```java
Statement statement = new SimpleStatement("INSERT INTO t1 (c1, c2) values ('foo', 'bar')")
// correct
Statement batch = new BatchStatement().add(statement);
batch.setCustomPayload(customPayload);
session.execute(batch);  //payload will be sent with BATCH request
// wrong
statement.setCustomPayload(customPayload);
Statement batch = new BatchStatement().add(statement);
session.execute(batch); //payload will NOT be sent with BATCH request
```

One important note about paging requests: if your query needs to paginate, Mes
any outgoing payload set on the executed statement *will be transparently M
sent over and over for every new page request*.

```java
Statement statement = new SimpleStatement("...");
statement.setCustomPayload(customPayload);
ResultSet rows = session.execute(...); //payload will be sent with first QUERY request
for (Row row : rows) {
    // if additional QUERY requests are sent, all of them will send the same payload
}
```

And finally, note that custom payloads can only be used with protocol versions >= 4; 
trying to set a payload under lower protocol versions will result in 
an [UnsupportedFeatureException][ufe] (wrapped in a [NoHostAvailableException][nhae])
when the request is encoded.

[ufe]:http://docs.datastax.com/en/drivers/java/2.2/com/datastax/driver/core/exceptions/UnsupportedFeatureException.html

If you want to defensively protect your code against these errors, you can either:

1) Force the protocol version when creating your `Cluster` instance:

```java
Cluster cluster = Cluster.builder()
    .addContactPoint("...")
    .withProtocolVersion(ProtocolVersion.V4)
    .build();
```

2) Inspect the current negotiated protocol version, and only send payloads if version is equal to or
greater than 4:

```java
ProtocolVersion myCurrentVersion = cluster.getConfiguration()
    .getProtocolOptions()
    .getProtocolVersionEnum();
if (myCurrentVersion.compareTo(ProtocolVersion.V4) >= 0) {
    // only send custom payloads if current protocol version supports it
    statement.setCustomPayload(customPayload);
}
```

### Receiving custom payloads
 
Custom payloads sent back by the server can be retrieved in the following ways:

1) From a `ResultSet`: use the `ExecutionInfo` class to retrieve the payload sent by the server.

```java
Statement statement = new SimpleStatement("...");
ResultSet rows = session.execute(...);
// last payload sent by the server
Map<String,byte[]> serverPayload = rows.getExecutionInfo().getCustomPayload();
```

If your query required pagination (multiple `QUERY` requests),
the above method would only return the last payload sent by server
with its last `RESULT` response. If you need to retrieve all the 
payloads for all `RESULT` responses, use the following method instead:

```java
Statement statement = new SimpleStatement("...");
ResultSet rows = session.execute(...);
//all payloads sent by the server
for (ExecutionInfo info : rows.getAllExecutionInfo()) {
    Map<String,byte[]> serverPayload = info.getCustomPayload();
}
```

2) From a `PreparedStatement`: to retrieve the payload that the server sent back
with its `PREPARED` response, use the following method:

```java
PreparedStatement ps = session.prepare(...);
Map<String,byte[]> serverPayload = ps.getCustomPayload();
```

### Debugging custom payloads

When debugging custom payloads, set the `com.datastax.driver.core.Message` logger to the `TRACE` level, e.g. with Log4J:
                                                                                                              
```xml
<logger name="com.datastax.driver.core.Message">
  <level value="TRACE"/>
</logger>
```

This will log a message for every request and every response that contains a non-null payload. 
The log message contains a pretty-printed version of the payload itself, and its total length in bytes.
