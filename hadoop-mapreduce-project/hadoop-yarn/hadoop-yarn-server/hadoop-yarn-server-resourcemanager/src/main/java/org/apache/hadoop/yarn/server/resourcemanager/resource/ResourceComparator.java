package org.apache.hadoop.yarn.server.resourcemanager.resource;

import java.util.Comparator;

import org.apache.hadoop.yarn.api.records.Resource;

public interface ResourceComparator extends Comparator<Resource> {

  void setClusterResource(Resource clusterResource);
  
}
