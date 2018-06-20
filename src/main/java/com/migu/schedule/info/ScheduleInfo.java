package com.migu.schedule.info;

import java.util.*;

public class ScheduleInfo implements Cloneable{
    private Integer nodeId;
    /**
     *总消耗
     */
    private Integer sumConsumption = 0;

    private Map<Integer/*index*/, Integer/*taskId*/> taskIdIndexMap = new LinkedHashMap<Integer, Integer>();
    /**
     * 任务Id集合
     */
    private Set<Integer> taskIdSet = new TreeSet<Integer>();

    public ScheduleInfo()
    {
    }

    public ScheduleInfo(int nodeId)
    {
        this.nodeId = nodeId;
    }

    public ScheduleInfo(Integer sumConsumption, Set<Integer> taskIdSet)
    {
        this.sumConsumption = sumConsumption;
        this.taskIdSet.addAll(taskIdSet);
        initTaskIdIndexMap();
    }

    public Integer getNodeId() {
        return nodeId;
    }

    public void setNodeId(Integer nodeId) {
        this.nodeId = nodeId;
    }

    public Integer getSumConsumption() {
        return sumConsumption;
    }

    public void setSumConsumption(Integer sumConsumption) {
        this.sumConsumption = sumConsumption;
    }

    public Set<Integer> getTaskIdSet() {
        return taskIdSet;
    }

    public void setTaskIdSet(Set<Integer> taskIdSet) {
        this.taskIdSet.clear();
        this.taskIdSet.addAll(taskIdSet);
        initTaskIdIndexMap();
    }

    public void addTaskId(Integer taskId)
    {
        this.taskIdSet.add(taskId);
        initTaskIdIndexMap();
    }

    public void removeTaskId(Integer taskId)
    {
        this.taskIdSet.remove(taskId);
        initTaskIdIndexMap();
    }

    /**
     * 根据taskIdSet初始化taskIdIndexMap
     */
    public void initTaskIdIndexMap()
    {
        taskIdIndexMap.clear();
        Integer index = 0;
        for (Integer taskId : taskIdSet)
        {
            taskIdIndexMap.put(index, taskId);
            index +=1;
        }
    }

    @Override
    public Object clone() {
        ScheduleInfo item = new ScheduleInfo();
        Map<Integer/*index*/, Integer/*taskId*/> cTaskIdIndexMap = new HashMap<Integer, Integer>();
        Set<Integer> cTaskIdSet = new TreeSet<Integer>();
        item.setSumConsumption(sumConsumption);
        cTaskIdIndexMap.putAll(taskIdIndexMap);
        cTaskIdSet.addAll(taskIdSet);
        item.setTaskIdSet(cTaskIdSet);
        item.setTaskIdIndexMap(cTaskIdIndexMap);
        return item;
    }

    public Integer getMaxTaskId()
    {
        return taskIdIndexMap.get(taskIdIndexMap.size() -1);
    }

    public Integer getMinTaskId()
    {
        return  taskIdIndexMap.get(0);
    }

    public Map<Integer, Integer> getTaskIdIndexMap() {
        return taskIdIndexMap;
    }

    public void setTaskIdIndexMap(Map<Integer, Integer> taskIdIndexMap) {
        this.taskIdIndexMap = taskIdIndexMap;
    }

    public String toString()
    {
        StringBuilder str = new StringBuilder();
        str.append("nodeId:").append(nodeId).append(", sumConsumption:").append(sumConsumption);
        str.append(",taskIdSet:[");
        for (Integer taskId : taskIdSet)
        {
            str.append(taskId).append(",");
        }
        str.append("], ");
        str.append(",taskIdIndexMap:[");
        for (Map.Entry<Integer,Integer> entry : taskIdIndexMap.entrySet())
        {
            str.append(entry.getValue()).append("=").append(entry.getValue()).append(",");
        }
        str.append("], ");
        return str.toString();
    }
}
