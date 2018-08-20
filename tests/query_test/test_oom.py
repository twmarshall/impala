import pytest
import subprocess as sp

from tests.common.impala_test_suite import ImpalaTestSuite

class TestOOM(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @pytest.mark.execute_serially
  def test_oom(self, vector, cursor):
    self.run_test_case("QueryTest/oom", vector)
