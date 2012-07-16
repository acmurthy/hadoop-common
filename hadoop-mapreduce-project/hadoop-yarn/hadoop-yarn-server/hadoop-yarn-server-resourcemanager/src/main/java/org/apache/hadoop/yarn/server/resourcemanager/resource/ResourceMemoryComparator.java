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

public class ResourceMemoryComparator extends ResourceComparator {

  Resource clusterResource;
  
  @Override
  public int compare(Resource lhs, Resource rhs) {
    // Only consider memory
    return lhs.getMemory() - rhs.getMemory();
  }

  @Override
  public void setClusterResource(Resource clusterResource) {
    this.clusterResource = clusterResource;
  }

  @Override
  public int computeAvailableContainers(Resource available, Resource required) {
    // Only consider memory
    return available.getMemory() / required.getMemory();
  }

  @Override
  public float divide(Resource lhs, Resource rhs) {
    return (float)lhs.getMemory() / rhs.getMemory();
  }

  @Override
  public float divideBy(Resource lhs, Resource rhs) {
    return divide(lhs, rhs);
  }

  @Override
  public Resource divideAndCeil(Resource lhs, int rhs) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Resource roundUp(Resource lhs, Resource rhs) {
    return Resources.createResource(
        divideAndCeil(lhs.getMemory(), rhs.getMemory()) * rhs.getMemory()); 
  }

  @Override
  public Resource roundDown(Resource lhs, Resource rhs) {
    return Resources.createResource(
        (lhs.getMemory() / rhs.getMemory()) * rhs.getMemory());
  }

}
