#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import print_function

import csv
import re
import sys

try:
  from cStringIO import StringIO  # python 2
except ImportError:
  from io import StringIO  # python 3


class PrettyOutputFormatter(object):
  def __init__(self, prettytable):
    self.prettytable = prettytable

  def format(self, rows):
    """Returns string containing UTF-8-encoded representation of the table data."""
    # Clear rows that already exist in the table.
    self.prettytable.clear_rows()
    try:
      map(self.prettytable.add_row, rows)
      # PrettyTable.get_string() converts UTF-8-encoded strs added via add_row() into
      # Python unicode strings. We need to convert it back to a UTF-8-encoded str for
      # output, since Python won't do the encoding automatically when outputting to a
      # non-terminal (see IMPALA-2717).
      return self.prettytable.get_string().encode('utf-8')
    except Exception as e:
      # beeswax returns each row as a tab separated string. If a string column
      # value in a row has tabs, it will break the row split. Default to displaying
      # raw results. This will change with a move to hiveserver2. Reference: IMPALA-116
      error_msg = ("Prettytable cannot resolve string columns values that have "
                   "embedded tabs. Reverting to tab delimited text output")
      print(error_msg, file=sys.stderr)
      print('{0}: {1}'.format(type(e), str(e)), file=sys.stderr)
      return '\n'.join(['\t'.join(row) for row in rows])


class DelimitedOutputFormatter(object):
  def __init__(self, field_delim="\t"):
    if field_delim:
      self.field_delim = field_delim.decode('string-escape')
      # IMPALA-8652, the delimiter should be a 1-character string and verified already
      assert len(self.field_delim) == 1

  def format(self, rows):
    """Returns string containing UTF-8-encoded representation of the table data."""
    # csv.writer expects a file handle to the input.
    # cStringIO is used as the temporary buffer.
    temp_buffer = StringIO()
    writer = csv.writer(temp_buffer, delimiter=self.field_delim,
                        lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
    writer.writerows(rows)
    rows = temp_buffer.getvalue().rstrip('\n')
    temp_buffer.close()
    return rows


class OutputStream(object):
  def __init__(self, formatter, filename=None):
    """Helper class for writing query output.

    User should invoke the `write(data)` method of this object.
    `data` is a list of lists.
    """
    self.formatter = formatter
    self.handle = sys.stdout
    self.filename = filename
    if self.filename:
      try:
        self.handle = open(self.filename, 'ab')
      except IOError as err:
        print("Error opening file %s: %s" % (self.filename, str(err)),
              file=self.handle)
        print(sys.stderr, "Writing to stdout", file=self.handle)

  def write(self, data):
    print(self.formatter.format(data), file=self.handle)
    self.handle.flush()

  def __del__(self):
    # If the output file cannot be opened, OutputStream defaults to sys.stdout.
    # Don't close the file handle if it points to sys.stdout.
    if self.filename and self.handle != sys.stdout:
      self.handle.close()


class OverwritingStdErrOutputStream(object):
  """This class is used to write output to stderr and overwrite the previous text as
  soon as new content needs to be written."""

  # ANSI Escape code for up.
  UP = "\x1b[A"

  def __init__(self):
    self.last_line_count = 0
    self.last_clean_text = ""

  def _clean_before(self):
    sys.stderr.write(self.UP * self.last_line_count)
    sys.stderr.write(self.last_clean_text)

  def write(self, data):
    """This method will erase the previously printed text on screen by going
    up as many new lines as the old text had and overwriting it with whitespace.
    Afterwards, the new text will be printed."""
    self._clean_before()
    new_line_count = data.count("\n")
    sys.stderr.write(self.UP * min(new_line_count, self.last_line_count))
    sys.stderr.write(data)

    # Cache the line count and the old text where all text was replaced by
    # whitespace.
    self.last_line_count = new_line_count
    self.last_clean_text = re.sub(r"[^\s]", " ", data)

  def clear(self):
    sys.stderr.write(self.UP * self.last_line_count)
    sys.stderr.write(self.last_clean_text)
    sys.stderr.write(self.UP * self.last_line_count)
    self.last_line_count = 0
    self.last_clean_text = ""
