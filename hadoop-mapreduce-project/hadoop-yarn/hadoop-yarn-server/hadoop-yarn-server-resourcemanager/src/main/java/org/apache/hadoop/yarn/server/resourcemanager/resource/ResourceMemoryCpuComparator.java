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

public class ResourceMemoryCpuComparator extends ResourceComparator {

  private Resource clusterResource;
  
  @Override
  public int compare(Resource lhs, Resource rhs) {
    float l = getResourceAsValue(lhs);
    float r = getResourceAsValue(rhs);
    
    if (l < r) {
      return -1;
    } else if (l > r) {
      return 1;
    }
    
    return 0;
  }

  protected float getResourceAsValue(Resource resource) {
    // Just use 'dominant' resource
    return Math.max(
        (float)resource.getMemory() / clusterResource.getMemory(), 
        (float)resource.getCores() / clusterResource.getCores()
        );
  }
  
  @Override
  public void setClusterResource(Resource clusterResource) {
    this.clusterResource = clusterResource;
  }

  @Override
  public int computeAvailableContainers(Resource available, Resource required) {
    return Math.min(
        available.getMemory() / required.getMemory(), 
        available.getCores() / required.getCores());
  }

  @Override
  public float divide(Resource lhs, Resource rhs) {
    return getResourceAsValue(lhs) / getResourceAsValue(rhs);
  }

  @Override
  public float divideBy(Resource lhs, Resource rhs) {
    return Math.max(
        (float)lhs.getMemory()/rhs.getMemory(), 
        (float)lhs.getCores()/rhs.getCores()
        );
  }

  @Override
  public Resource divideAndCeil(Resource lhs, int rhs) {
    return Resources.createResource(
        divideAndCeil(lhs.getMemory(), rhs),
        divideAndCeil(lhs.getCores(), rhs)
        );
  }

  @Override
  public Resource roundUp(Resource lhs, Resource rhs) {
    return Resources.createResource(
        divideAndCeil(lhs.getMemory(), rhs.getMemory()) * rhs.getMemory(),
        divideAndCeil(lhs.getCores(), rhs.getCores()) * rhs.getCores()
        ); 
  }

  @Override
  public Resource roundDown(Resource lhs, Resource rhs) {
    return Resources.createResource(
        (lhs.getMemory() / rhs.getMemory()) * rhs.getMemory(),
        (lhs.getCores() / rhs.getCores()) * rhs.getCores()
        );
  }

}
