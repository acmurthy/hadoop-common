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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

@Private
@Evolving
public class Resources {
  
  // Java doesn't have const :(
  private static final Resource NONE = new Resource() {

    @Override
    public int getMemory() {
      return 0;
    }

    @Override
    public void setMemory(int memory) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    public int getCores() {
      return 0;
    }

    @Override
    public void setCores(int cores) {
      throw new RuntimeException("NONE cannot be modified!");
    }

    @Override
    public int compareTo(Resource o) {
      int diff = 0 - o.getMemory();
      if (diff == 0) {
        diff = 0 - o.getCores();
      }
      return diff;
    }
    
  };

  public static Resource createResource(int memory) {
    return createResource(memory, 1);
  }

  public static Resource createResource(int memory, int cores) {
    Resource resource = Records.newRecord(Resource.class);
    resource.setMemory(memory);
    resource.setCores(1);
    return resource;
  }

  public static Resource none() {
    return NONE;
  }

  public static Resource clone(Resource res) {
    return createResource(res.getMemory(), res.getCores());
  }

  public static Resource addTo(Resource lhs, Resource rhs) {
    lhs.setMemory(lhs.getMemory() + rhs.getMemory());
    lhs.setCores(lhs.getCores() + rhs.getCores());
    return lhs;
  }

  public static Resource add(Resource lhs, Resource rhs) {
    return addTo(clone(lhs), rhs);
  }

  public static Resource subtractFrom(Resource lhs, Resource rhs) {
    lhs.setMemory(lhs.getMemory() - rhs.getMemory());
    lhs.setCores(lhs.getCores() - rhs.getCores());
    return lhs;
  }

  public static Resource subtract(Resource lhs, Resource rhs) {
    return subtractFrom(clone(lhs), rhs);
  }

  public static Resource negate(Resource resource) {
    return subtract(NONE, resource);
  }

  public static Resource multiplyTo(Resource lhs, int by) {
    lhs.setMemory(lhs.getMemory() * by);
    lhs.setCores(lhs.getCores() * by);
    return lhs;
  }

  public static Resource multiply(Resource lhs, int by) {
    return multiplyTo(clone(lhs), by);
  }
  
  /**
   * Mutliply a resource by a {@code double}. Note that integral 
   * resource quantites are subject to rounding during cast.
   */
  public static Resource multiply(Resource lhs, double by) {
    Resource out = clone(lhs);
    out.setMemory((int) (lhs.getMemory() * by));
    return out;
  }

  public static float divide(ResourceComparator comparator,
      Resource lhs, Resource rhs) {
    return comparator.divide(lhs, rhs);
  }
  
  public static boolean equals(Resource lhs, Resource rhs) {
    return lhs.equals(rhs);
  }

  public static boolean lessThan(
      ResourceComparator comparator, 
      Resource lhs, Resource rhs) {
    return (comparator.compare(lhs, rhs) < 0);
  }

  public static boolean lessThanOrEqual(
      ResourceComparator comparator, 
      Resource lhs, Resource rhs) {
    return (comparator.compare(lhs, rhs) <= 0);
  }

  public static boolean greaterThan(
      ResourceComparator comparator, 
      Resource lhs, Resource rhs) {
    return comparator.compare(lhs, rhs) > 0;
  }

  public static boolean greaterThanOrEqual(
      ResourceComparator comparator, 
      Resource lhs, Resource rhs) {
    return comparator.compare(lhs, rhs) >= 0;
  }
  
  public static Resource min(
      ResourceComparator comparator, 
      Resource lhs, Resource rhs) {
    return comparator.compare(lhs, rhs) <= 0 ? lhs : rhs;
  }

  public static Resource max(
      ResourceComparator comparator, 
      Resource lhs, Resource rhs) {
    return comparator.compare(lhs, rhs) >= 0 ? lhs : rhs;
  }
}
