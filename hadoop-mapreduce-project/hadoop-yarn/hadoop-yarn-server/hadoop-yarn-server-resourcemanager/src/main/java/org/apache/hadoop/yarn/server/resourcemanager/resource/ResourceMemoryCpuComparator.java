/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.hadoop.yarn.server.resourcemanager.resource;

import org.apache.hadoop.yarn.api.records.Resource;

public class ResourceMemoryCpuComparator implements ResourceComparator {

  private Resource clusterResource;
  
  @Override
  public int compare(Resource lhs, Resource rhs) {
    float l = getDominantResource(lhs);
    float r = getDominantResource(rhs);
    
    if (l < r) {
      return -1;
    } else if (l > r) {
      return 1;
    }
    
    return 0;
  }

  private float getDominantResource(Resource lhs) {
    return Math.max(
        (float)lhs.getMemory() / clusterResource.getMemory(), 
        (float)lhs.getCores() / clusterResource.getCores()
        );
  }
  
  @Override
  public void setClusterResource(Resource clusterResource) {
    this.clusterResource = clusterResource;
  }

}
