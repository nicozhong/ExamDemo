package com.migu.schedule.util;

import com.migu.schedule.info.ScheduleInfo;
import com.migu.schedule.info.TaskInfoExt;

import java.util.*;

public class ScheduleTaskUtil {

    /**
     * 判断是否查过阈值
     * @param scheduleInfoMap
     * @param taskInfoMap
     * @param threshold
     * @return
     */
    public static boolean checkExceedThreshold(Map<Integer, ScheduleInfo> scheduleInfoMap,
                                               Map<Integer, TaskInfoExt> taskInfoMap,
                                               Integer threshold)
    {
        int nodeSize = scheduleInfoMap.size();
        List<ScheduleInfo> scheduleInfoList = sortScheduleInfo(scheduleInfoMap, taskInfoMap);
        ScheduleInfo tScheduleInfo = scheduleInfoList.get(0);
        ScheduleInfo sScheduleInfo = scheduleInfoList.get(nodeSize -1);
        int gap = sScheduleInfo.getSumConsumption() - tScheduleInfo.getSumConsumption();
        boolean rc = gap > threshold;
        return rc;
    }

    /**
     * 对nodeId按功耗进行排序
     * @return
     */
    public static List<ScheduleInfo> sortScheduleInfo(Map<Integer, ScheduleInfo> scheduleInfoMap,
                                                       Map<Integer, TaskInfoExt> taskInfoMap)
    {
        List<ScheduleInfo> scheduleInfoList = new ArrayList<ScheduleInfo>();
        Collection<ScheduleInfo> values =  scheduleInfoMap.values();
        Iterator<ScheduleInfo> it = values.iterator();
        while(it.hasNext()) {
            ScheduleInfo item = it.next();
            calcSumConsumption(item, taskInfoMap);
            scheduleInfoList.add(item);
        }
        Collections.sort(scheduleInfoList, new Comparator<ScheduleInfo>() {
            public int compare(ScheduleInfo item1, ScheduleInfo item2) {
                int rc = item1.getSumConsumption() - item2.getSumConsumption();
                if (0 == rc)
                {
                    rc = item1.getNodeId() - item2.getNodeId();
                }
                return  rc;
            }
        });
        return scheduleInfoList;
    }

    /**
     * 计算scheduleInfo
     * @param scheduleInfo
     */
    public static void calcSumConsumption(ScheduleInfo scheduleInfo, Map<Integer, TaskInfoExt> taskInfoMap)
    {
        Set<Integer> taskIdSet = scheduleInfo.getTaskIdSet();
        if (null != taskIdSet && taskIdSet.size() > 0)
        {
            int sumConsumption = 0;
            for (Integer taskId : taskIdSet)
            {
                TaskInfoExt taskInfoExt = taskInfoMap.get(taskId);
                sumConsumption += (null != taskInfoExt ? taskInfoExt.getConsumption() : 0);
            }
            scheduleInfo.setSumConsumption(sumConsumption);
        }
    }

    /**
     * 拷贝scheduleInfoMap
     * @param srcScheduleInfoMap
     * @param destScheduleInfoMap
     */
    public static void copyScheduleInfoMap(Map<Integer, ScheduleInfo> srcScheduleInfoMap, Map<Integer, ScheduleInfo> destScheduleInfoMap)
    {
        for (Map.Entry<Integer, ScheduleInfo> entry : srcScheduleInfoMap.entrySet())
        {
            ScheduleInfo item = (ScheduleInfo)entry.getValue().clone();
            destScheduleInfoMap.put(entry.getKey(), item);
        }
    }

    /**
     * 拷贝TaskInfoMap
     * @param srcTaskInfoMap
     * @param destTaskInfoMap
     */
    public static void copyTaskInfoMap(Map<Integer, TaskInfoExt> srcTaskInfoMap, Map<Integer, TaskInfoExt> destTaskInfoMap)
    {
        for (Map.Entry<Integer, TaskInfoExt> entry : srcTaskInfoMap.entrySet())
        {
            TaskInfoExt item = (TaskInfoExt)entry.getValue().clone();
            destTaskInfoMap.put(entry.getKey(), item);
        }
    }

    /**
     * 根据nodeId获取ScheduleInfo，如果没有则new对象放进scheduleInfoMap
     * @param scheduleInfoMap
     * @param nodeId
     * @return
     */
    public static ScheduleInfo getScheduleInfo(Map<Integer, ScheduleInfo> scheduleInfoMap, Integer nodeId)
    {
        ScheduleInfo scheduleInfo = scheduleInfoMap.get(nodeId);
        if (null == scheduleInfo)
        {
            scheduleInfo = new ScheduleInfo();
            scheduleInfoMap.put(nodeId, scheduleInfo);
        }
        return scheduleInfo;
    }

    /**
     * 设置task的nodeId
     * @param taskId
     * @param nodeId
     */
    public static void setTaskNodeId(Map<Integer, TaskInfoExt> taskInfoMap, Integer taskId, Integer nodeId)
    {
        TaskInfoExt taskInfoExt = taskInfoMap.get(taskId);
        if (null != taskInfoExt)
        {
            taskInfoExt.setNodeId(nodeId);
        }
    }
}
