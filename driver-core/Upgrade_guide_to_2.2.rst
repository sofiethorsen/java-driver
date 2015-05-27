Upgrade Guide to 2.2
====================

The purpose of this guide is to detail the changes made by version 2.2 of
the Java Driver.

2.2.0-rc1
---------

User API Changes
~~~~~~~~~~~~~~~~

1. Custom paylaods (JAVA-779). The methods listed below were added to the public API.
    ``Statement`` and ``ExecutionInfo`` classes remain binary-compatible,
    but if you have custom implementations of ``PreparedStatement`` or ``AbstractSession``,
    you will have to adapt your implementations accordingly. Note that custom payloads can only
    be used with protocol versions >= 4; trying to set a payload under lower protocol
    versions will result in an ``UnsupportedFeatureException``.
   1. ``Statement#getCustomPayload()`` and ``Statement#setCustomPayload(Map<String,byte[]>)``
   2. ``PreparedStatement#getCustomPayload()``
   3. ``AbstractSession#prepareAsync(String, Map<String,byte[]>)``
   4. ``ExecutionInfo#getCustomPayload()``


