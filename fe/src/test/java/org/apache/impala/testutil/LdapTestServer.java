// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.testutil;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.integ.CreateLdapServerRule;
import org.junit.ClassRule;
import org.junit.Test;

@CreateDS(name = "myDS",
    partitions = { @CreatePartition(name = "test", suffix = "dc=myorg,dc=com") })
@CreateLdapServer(
    transports = { @CreateTransport(protocol = "LDAP", address = "localhost", port = 22010) })
    @ApplyLdifFiles({"users.ldif"})
public class LdapTestServer {
  @ClassRule
  public static CreateLdapServerRule serverRule = new CreateLdapServerRule();

  @Test
  public void test() throws Exception {
    // The test LDAP server we're using from ApacheDS was designed to be used with junit.
    // This is a hack to make it run as a daemon so that we can write non-junit tests (eg.
    // C++ unit tests) that use it.
    Lock lock = new ReentrantLock();
    Condition cond = lock.newCondition();
    lock.lock();
    cond.await();
  }
}
