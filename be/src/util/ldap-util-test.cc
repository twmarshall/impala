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

#include "testutil/gtest-util.h"

#include "testutil/scoped-flag-setter.h"
#include "util/ldap-util.h"
#include "util/os-util.h"

DECLARE_string(ldap_uri);
DECLARE_string(ldap_bind_pattern);
DECLARE_string(ldap_bind_dn);
DECLARE_string(ldap_bind_password_cmd);
DECLARE_string(ldap_group_search_pattern);

namespace impala {

TEST(LdapUtilTest, TestLdap) {
  auto ldap_uri = ScopedFlagSetter<string>::Make(&FLAGS_ldap_uri, "ldap://localhost:22010");
  auto ldap_bind_pattern = ScopedFlagSetter<string>::Make(&FLAGS_ldap_bind_pattern, "cn=#UID,ou=Users,dc=myorg,dc=com");

  // Initialize on ImpalaLdap with no filters.
  ImpalaLdap ldap;
  EXPECT_OK(ldap.Init("", ""));
  // Empty username should fail.
  EXPECT_FALSE(ldap.LdapCheckPass("", "", 0));
  // Valid username/password combination should succeed.
  EXPECT_TRUE(ldap.LdapCheckPass("Test1Ldap", "12345", 5));

  // LdapCheckFilters should always succeed since no filters are enabled.
  EXPECT_TRUE(ldap.LdapCheckFilters(""));
  EXPECT_TRUE(ldap.LdapCheckFilters("invalid-user"));
  EXPECT_TRUE(ldap.LdapCheckFilters("TestlLdap"));
}

TEST(LdapUtilTest, TestGroupSearchPattern) {
  auto ldap_uri = ScopedFlagSetter<string>::Make(&FLAGS_ldap_uri, "ldap://localhost:22010");
  auto ldap_bind_pattern = ScopedFlagSetter<string>::Make(&FLAGS_ldap_bind_pattern, "cn=#UID,ou=Users,dc=myorg,dc=com");
  auto ldap_bind_dn = ScopedFlagSetter<string>::Make(&FLAGS_ldap_bind_dn, "Test1Ldap");
  auto ldap_bind_password_cmd = ScopedFlagSetter<string>::Make(&FLAGS_ldap_bind_password_cmd, "echo 12345");

  {
    auto ldap_group_search_pattern = ScopedFlagSetter<string>::Make(&FLAGS_ldap_group_search_pattern, "invalid");
    ImpalaLdap ldap;
    EXPECT_OK(ldap.Init("asdf", ""));
    EXPECT_FALSE(ldap.LdapCheckFilters(""));
  }
}

} // namespace impala
