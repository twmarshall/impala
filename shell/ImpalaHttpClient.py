#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

from io import BytesIO
import os
import requests
import ssl
import sys
import warnings
import base64

from six.moves import urllib
from six.moves import http_client

from requests.packages.urllib3.exceptions import InsecureRequestWarning
from thrift.transport.TTransport import TTransportBase
from shell_exceptions import RPCException
import six

# This was taken from THttpClient.py in Thrift to allow making changes Impala needs.
# The current changes that have been applied:
# - Added logic for the 'Expect: 100-continue' header on large requests
# - If an error code is received back in flush(), an exception is thrown.
# Note there is a copy of this code in Impyla.
class ImpalaHttpClient(TTransportBase):
  """Http implementation of TTransport base."""

  # When sending requests larger than this size, include the 'Expect: 100-continue' header
  # to indicate to the server to validate the request before reading the contents. This
  # value was chosen to match curl's behavior. See Section 8.2.3 of RFC2616.
  MIN_REQUEST_SIZE_FOR_EXPECT = 1024

  def __init__(self, uri, cafile=None):
    """'uri' is expected to be in the form {scheme}://{host}:{port}/{path}"""
    self.uri = uri
    self.verify = False if cafile is None else cafile
    if not self.verify:
      # Disable the warning about SSL not being verified. We already print a warning when impala-shell first starts,
      # and this warning gets printed on every request so its very noisey.
      requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

    self.__wbuf = BytesIO()
    self.__http_response = None
    self.__timeout = None
    self.__custom_headers = None
    self.__session = None

  def open(self):
    self.__session = requests.Session()
    self.__session.headers['Content-Type'] = 'application/x-thrift'
    if not self.__custom_headers or 'User-Agent' not in self.__custom_headers:
      user_agent = 'Python/ImpalaHttpClient'
      script = os.path.basename(sys.argv[0])
      if script:
        user_agent = '%s (%s)' % (user_agent, urllib.parse.quote(script))
      self.__session.headers['User-Agent'] = user_agent

  def close(self):
    self.__session.close()
    self.__session = None

  def isOpen(self):
    return self.__session is not None

  def setTimeout(self, ms):
    if ms is None:
      self.__timeout = None
    else:
      self.__timeout = ms / 1000.0

  def setCustomHeaders(self, headers):
    self.__custom_headers = headers

  def read(self, sz):
    return self.__http_response.read(sz)

  def readBody(self):
    return self.__http_response.read()

  def write(self, buf):
    self.__wbuf.write(buf)

  def flush(self):
    # Pull data out of buffer
    data = self.__wbuf.getvalue()
    self.__wbuf = BytesIO()

    # Write headers
    headers = self.__custom_headers
    data_len = len(data)
    if data_len > ImpalaHttpClient.MIN_REQUEST_SIZE_FOR_EXPECT:
      # Add the 'Expect' header to large requests. Note that we do not explicitly wait for
      # the '100 continue' response before sending the data - 'requests' simply ignores
      # these types of responses, but we'll get the right behavior anyways.
      headers["Expect"] = "100-continue"

    # Write payload
    response = self.__session.post(self.uri, data=data, headers=headers, verify=self.verify)

    # Get reply to flush the request
    self.__http_response = BytesIO(response.content)
    self.code = response.status_code
    self.message = response.text
    self.headers = response.headers

    if self.code >= 300:
      # Report any http response code that is not 1XX (informational response) or
      # 2XX (successful).
      body = self.readBody()
      if not body:
        raise RPCException("HTTP code {}: {}".format(self.code, self.message))
      else:
        raise RPCException("HTTP code {}: {} [{}]".format(self.code, self.message, body))
