# Copyright (C) 2011-2012  The Async HBase Authors.  All rights reserved.
# This file is part of Async HBase.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#   - Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
#   - Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
#   - Neither the name of the StumbleUpon nor the names of its contributors
#     may be used to endorse or promote products derived from this software
#     without specific prior written permission.
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

HBASE_VERSION := 1.0.1

HBASE_CLIENT_VERSION := $(HBASE_VERSION)
HBASE_CLIENT := third_party/hbase-client/hbase-client-$(HBASE_CLIENT_VERSION).jar
HBASE_CLIENT_URL := http://central.maven.org/maven2/org/apache/hbase/hbase-client/$(HBASE_CLIENT_VERSION)

$(HBASE_CLIENT): $(HBASE_CLIENT).md5
	set dummy "$(HBASE_CLIENT_URL)" "$(HBASE_CLIENT)"; shift; $(FETCH_DEPENDENCY)


HBASE_COMMON_VERSION := $(HBASE_VERSION)
HBASE_COMMON := third_party/hbase-client/hbase-common-$(HBASE_COMMON_VERSION).jar
HBASE_COMMON_BASE_URL := http://central.maven.org/maven2/org/apache/hbase/hbase-common/$(HBASE_COMMON_VERSION)

$(HBASE_COMMON): $(HBASE_COMMON).md5
	set dummy "$(HBASE_COMMON_BASE_URL)" "$(HBASE_COMMON)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(HBASE_COMMON) $(HBASE_CLIENT)
