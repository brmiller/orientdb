/*
 * Copyright 2010-2013 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.hazelcast.strategy;

import com.hazelcast.core.Member;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.OReplicationStrategy;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;

/**
 * Automatic replication strategy: returns the node following the Hazelcast DHT computed on key or cluster name if key is null.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OAutoReplicationStrategy implements OReplicationStrategy {
  @Override
  public String getMasterNode(final ODistributedServerManager iManager, final String iClusterName, final Object iKey) {
    final Member member;
    if (iKey != null)
      // GET THE PARTITION BASED ON THE KEY
      member = ((OHazelcastPlugin) iManager).getHazelcastInstance().getPartitionService().getPartition(iKey).getOwner();
    else if (iClusterName != null)
      // GET THE PARTITION BASED ON THE CLUSTER NAME
      member = ((OHazelcastPlugin) iManager).getHazelcastInstance().getPartitionService().getPartition(iClusterName).getOwner();
    else
      // GET THE LOCAL NODE
      member = ((OHazelcastPlugin) iManager).getHazelcastInstance().getCluster().getLocalMember();

    return ((OHazelcastPlugin) iManager).getNodeId(member);
  }
}
