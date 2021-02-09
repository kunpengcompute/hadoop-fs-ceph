/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 *
 * Description: CephRgw implementation of AbstractFileSystem.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.cephrgw;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Defines the CephRgw class, which inherits the DelegateToFileSystem.
 * This impl delegates to the CephRgwFileSystem
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CephRgw extends DelegateToFileSystem {
    private static final int DEF_PORT = -1;

    public CephRgw(final URI theUri, final Configuration conf) throws IOException, URISyntaxException {
        super(theUri, new CephRgwFileSystem(), conf, CephRgwFileSystem.SCHEME, false);
    }

    @Override
    public int getUriDefaultPort() {
        return DEF_PORT;
    }
}
