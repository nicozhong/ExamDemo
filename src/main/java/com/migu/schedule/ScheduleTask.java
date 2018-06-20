package com.migu.schedule;

import com.migu.schedule.info.ScheduleInfo;
import com.migu.schedule.info.TaskInfoExt;
import com.migu.schedule.util.ScheduleTaskUtil;

import java.util.*;

public class ScheduleTask {
    private Map<Integer/*nodeId*/, ScheduleInfo> scheduleInfoMap = null;
    private Map<Integer/*taskId*/, TaskInfoExt> taskInfoMap = null;
    private Set<Integer/*taskId*/> taskHangSet = null;
    private Integer threshold;

    public void init(Map<Integer/*nodeId*/, ScheduleInfo> scheduleInfoMap,
                     Map<Integer/*taskId*/, TaskInfoExt> taskConsumptionMap,
                     Set<Integer/*taskId*/> taskHangSet,
                     Integer threshold)
    {
        this.scheduleInfoMap = scheduleInfoMap;
        this.taskInfoMap = taskConsumptionMap;
        this.taskHangSet = taskHangSet;
        this.threshold = threshold;
        initScheduleInfos();
    }

    /**
     * 计算scheduleInfo
     * @param scheduleInfo
     */
    public void calcSumConsumption(ScheduleInfo scheduleInfo)
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
     * 调度
     * @return
     */
    public int scheduleTask()
    {
        //备份
        Map<Integer, ScheduleInfo> oScheduleInfoMap = new HashMap<Integer, ScheduleInfo>();
        Map<Integer, TaskInfoExt> oTaskInfoMap = new HashMap<Integer, TaskInfoExt>();
        Set<Integer> oTaskHangSet = new HashSet<Integer>();
        ScheduleTaskUtil.copyScheduleInfoMap(scheduleInfoMap, oScheduleInfoMap);
        ScheduleTaskUtil.copyTaskInfoMap(taskInfoMap, oTaskInfoMap);
        oTaskHangSet.clear();
        oTaskHangSet.addAll(taskHangSet);

        int rc = 0;
        //处理挂起队列
        rc = handleTaskHang(oTaskHangSet);
        taskHangSet.clear();

        //调度缩小功耗插值
        int nodeSize = scheduleInfoMap.size();
        boolean isMigrate = true;
        int count = 0;
        List<ScheduleInfo> scheduleInfoList = sortScheduleInfo();
        ScheduleInfo tScheduleInfo = scheduleInfoList.get(0);
        for (ScheduleInfo scheduleInfo : scheduleInfoList)
        {
            ScheduleInfo sScheduleInfo = scheduleInfo;
            int nodeId = scheduleInfo.getNodeId();
            if (nodeId == tScheduleInfo.getNodeId())
            {
                continue;
            }

            Integer sTaskId = sScheduleInfo.getMinTaskId();
            //Integer sTaskId = getMinSumConsumptionTaskId(sScheduleInfo.getNodeId());
            int gap = sScheduleInfo.getSumConsumption() - tScheduleInfo.getSumConsumption();
            if (gap >  threshold && checkIsMigrateTask(tScheduleInfo, sScheduleInfo, sTaskId))
            {
                rc = 1;
                isMigrate = migrateTask(sTaskId, sScheduleInfo.getNodeId(), tScheduleInfo.getNodeId(), false);
            }
            else
            {
                isMigrate = false;
            }
            count ++;
            System.out.println(new StringBuilder("line:").append(88)
                    .append(",count:").append(count).toString());
        }

        for (ScheduleInfo scheduleInfo : scheduleInfoList)
        {
            System.out.println(scheduleInfo);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        for (ScheduleInfo scheduleInfo : scheduleInfoList)
        {
            System.out.println(scheduleInfo);
        }

        //处理maxGap > threshold情况
        for (Map.Entry<Integer, TaskInfoExt> entry : taskInfoMap.entrySet())
        {
            int consumption = entry.getValue().getConsumption();
            scheduleInfoList = sortScheduleInfo();
            ScheduleInfo minScheduleInfo = scheduleInfoList.get(0);
            ScheduleInfo maxScheduleInfo = scheduleInfoList.get(nodeSize -1);
            int maxGap = maxScheduleInfo.getSumConsumption() - minScheduleInfo.getSumConsumption();
            if(maxGap > consumption)
            {
                Integer sNodeId = maxScheduleInfo.getNodeId();
                Integer sTaskId = getMinSumConsumptionTaskId(sNodeId);
                isMigrate = migrateTask(sTaskId, sNodeId, minScheduleInfo.getNodeId(), false);
                rc = 1;
            }
        }

        //处理消耗都相同情况
        for (Map.Entry<Integer, TaskInfoExt> entry : taskInfoMap.entrySet())
        {
            int consumption = entry.getValue().getConsumption();
            scheduleInfoList = sortScheduleInfo();
            ScheduleInfo minScheduleInfo = scheduleInfoList.get(0);
            for (ScheduleInfo scheduleInfo : scheduleInfoList)
            {
                int nodeId = scheduleInfo.getNodeId();
                if (nodeId == minScheduleInfo.getNodeId())
                {
                    continue;
                }
                int maxGap = scheduleInfo.getSumConsumption() - minScheduleInfo.getSumConsumption();
                if(0 == maxGap)
                {
                    //遍历
                    Integer maxNodeId = scheduleInfo.getNodeId();
                    Integer maxNodeMinTaskId = scheduleInfo.getMinTaskId();
                    Integer minNodeId = minScheduleInfo.getNodeId();
                    Integer minNodeMaxTaskId = minScheduleInfo.getMaxTaskId();
                    TaskInfoExt maxNodeMinTaskInfoExt = taskInfoMap.get(maxNodeId);
                    TaskInfoExt minNodeMaxTaskInfoExt = taskInfoMap.get(minNodeId);
                    //需要相等才迁移
                    if (/*maxNodeId < minNodeId && */maxNodeMinTaskId < minNodeMaxTaskId && maxNodeMinTaskInfoExt.getConsumption().equals(minNodeMaxTaskInfoExt.getConsumption()))
                    {
                        isMigrate = migrateTask(maxNodeMinTaskId, maxNodeId, minNodeId, false);
                        isMigrate = migrateTask(minNodeMaxTaskId, minNodeId, maxNodeId, false);
                        rc = 1;
                    }

                }
            }
        }

        //判断是否超过阈值，如果超过阈值做还原处理
        if (ScheduleTaskUtil.checkExceedThreshold(scheduleInfoMap, taskInfoMap, threshold))
        {
            //超过阈值不能调度
            rc = 0;
            //还原
            ScheduleTaskUtil.copyScheduleInfoMap(oScheduleInfoMap, scheduleInfoMap);
            ScheduleTaskUtil.copyTaskInfoMap(oTaskInfoMap, taskInfoMap);
            taskHangSet.clear();
            taskHangSet.addAll(oTaskHangSet);
            return rc;
        }
        return rc;
    }

    /**
     * 调度挂起任务
     * @param oTaskHangSet
     * @return 0:没有调度 1：已调度 -1：不能调度
     */
    private int handleTaskHang(Set<Integer> oTaskHangSet)
    {
        int rc = 0;
        for (Integer taskId : taskHangSet)
        {
            List<ScheduleInfo> scheduleInfoList = sortScheduleInfo();
            ScheduleInfo tScheduleInfo = scheduleInfoList.get(0);
            addTask(taskId, tScheduleInfo.getNodeId());
            rc = 1;
        }
        //判断是否超过阈值

        return rc;
    }

    /**
     * 初始化scheduleInfoMap
     */
    private void initScheduleInfos()
    {
        for (Map.Entry<Integer, ScheduleInfo> entry : scheduleInfoMap.entrySet())
        {
            calcSumConsumption(entry.getValue());
        }
    }

    /**
     * 对nodeId按功耗进行排序
     * @return
     */
    private List<ScheduleInfo> sortScheduleInfo()
    {
        List<ScheduleInfo> scheduleInfoList = new ArrayList<ScheduleInfo>();
        Collection<ScheduleInfo> values =  scheduleInfoMap.values();
        Iterator<ScheduleInfo> it = values.iterator();
        while(it.hasNext()) {
            ScheduleInfo item = it.next();
            calcSumConsumption(item);
            scheduleInfoList.add(item);
        }
        Collections.sort(scheduleInfoList, new Comparator<ScheduleInfo>() {
            public int compare(ScheduleInfo item1, ScheduleInfo item2) {
                return  item1.getSumConsumption() - item2.getSumConsumption();
            }
        });
        return scheduleInfoList;
    }

    /**
     * 添加任务
     * @param nodeId
     */
    private void addTask(Integer taskId, Integer nodeId)
    {
        ScheduleInfo scheduleInfo = ScheduleTaskUtil.getScheduleInfo(scheduleInfoMap, nodeId);
        scheduleInfo.addTaskId(taskId);
        ScheduleTaskUtil.setTaskNodeId(taskInfoMap, taskId, nodeId);
        calcSumConsumption(scheduleInfo);
    }

    /**
     * 迁移任务
     * @param sTaskId
     * @param sNodeId
     * @param tNodeId
     */
    private boolean migrateTask(Integer sTaskId, Integer sNodeId, Integer tNodeId, boolean isCheck)
    {
        boolean isMigrate = false;
        ScheduleInfo sScheduleInfo = ScheduleTaskUtil.getScheduleInfo(scheduleInfoMap, sNodeId);
        ScheduleInfo tScheduleInfo = ScheduleTaskUtil.getScheduleInfo(scheduleInfoMap, tNodeId);
        if (isCheck && checkIsMigrateTask(tScheduleInfo, sScheduleInfo, sTaskId))
        {
            isMigrate = true;
        }
        else if (!isCheck)
        {
            isMigrate = true;
        }
        if (isMigrate)
        {
            sScheduleInfo.removeTaskId(sTaskId);
            calcSumConsumption(sScheduleInfo);
            tScheduleInfo.addTaskId(sTaskId);
            calcSumConsumption(tScheduleInfo);
        }
        return isMigrate;
    }

    /**
     * 查找nodeId上功耗最小的taskId
     * @param nodeId
     * @return
     */
    private Integer getMinSumConsumptionTaskId(Integer nodeId)
    {
        ScheduleInfo scheduleInfo = ScheduleTaskUtil.getScheduleInfo(scheduleInfoMap, nodeId);
        Set<Integer> taskIdSet = scheduleInfo.getTaskIdSet();
        Integer minTaskId = null;
        Integer sumConsumption = null;
        for (Integer taskId : taskIdSet)
        {
            TaskInfoExt taskInfoExt = taskInfoMap.get(taskId);
            if (null == taskInfoExt)
            {
                continue;
            }
            if (null == sumConsumption)
            {
                sumConsumption = taskInfoExt.getConsumption();
                minTaskId = taskId;
            }
            else if (taskInfoExt.getConsumption() < sumConsumption)
            {
                sumConsumption = taskInfoExt.getConsumption();
                minTaskId = taskId;
            }
        }
        return minTaskId;
    }

    /**
     * 确认是否可迁移
     * @param maxScheduleInfo
     * @param minScheduleInfo
     * @return
     */
    private boolean checkIsMigrateTaskWhenEqual(ScheduleInfo maxScheduleInfo, ScheduleInfo minScheduleInfo)
    {
        Integer maxNodeId = maxScheduleInfo.getNodeId();
        Integer maxNodeMinTaskId = maxScheduleInfo.getMinTaskId();
        Integer minNodeId = minScheduleInfo.getNodeId();
        Integer minNodeMaxTaskId = minScheduleInfo.getMaxTaskId();

        //默认可迁移
        boolean isMigrate = true;
        Integer maxConsumption = maxScheduleInfo.getSumConsumption();
        Integer minConsumption = minScheduleInfo.getSumConsumption();
        int gap = maxConsumption - minConsumption;
        Integer taskId = maxScheduleInfo.getMinTaskId();
        TaskInfoExt taskInfoExt = taskInfoMap.get(taskId);
        if (null != taskInfoExt)
        {
            minConsumption += taskInfoExt.getConsumption();
            maxConsumption -= taskInfoExt.getConsumption();
        }
        Integer newGap = Math.abs(maxConsumption - minConsumption);
        if (0 == maxConsumption)
        {
            isMigrate = false;
        } else if (newGap > gap)
        {
            isMigrate = false;
        } else if (newGap > threshold)
        {
            isMigrate = false;
        }
        return isMigrate;
    }


    /**
     * 确认是否可迁移
     * @param tScheduleInfo
     * @param sScheduleInfo
     * @return
     */
    private boolean checkIsMigrateTask(ScheduleInfo tScheduleInfo,
                                       ScheduleInfo sScheduleInfo,
                                       Integer sTaskId)
    {
        //默认可迁移
        boolean isMigrate = true;
        Integer sConsumption = sScheduleInfo.getSumConsumption();
        Integer tConsumption = tScheduleInfo.getSumConsumption();
        int gap = sConsumption - tConsumption;
        TaskInfoExt taskInfoExt = taskInfoMap.get(sTaskId);
        if (null != taskInfoExt)
        {
            tConsumption += taskInfoExt.getConsumption();
            sConsumption -= taskInfoExt.getConsumption();
        }
        Integer newGap = Math.abs(sConsumption - tConsumption);
        return isMigrate;
    }


}
