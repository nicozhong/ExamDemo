package com.migu.schedule;

import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.ScheduleInfo;
import com.migu.schedule.info.TaskInfo;
import com.migu.schedule.info.TaskInfoExt;

import java.util.*;

public class NodeTaskManage {
    private Map<Integer/*nodeId*/, ScheduleInfo> scheduleInfoMap = new HashMap<Integer, ScheduleInfo>();
    private Map<Integer/*taskId*/, TaskInfoExt> taskInfoMap = new HashMap<Integer, TaskInfoExt>();
    private Set<Integer/*taskId*/> taskHangSet = new HashSet<Integer>();
    public int init()
    {
        scheduleInfoMap.clear();
        taskInfoMap.clear();
        taskHangSet.clear();
        return ReturnCodeKeys.E001;
    }

    public int registerNode(int nodeId)
    {
        int rc = (nodeId <= 0 ? ReturnCodeKeys.E004 : ReturnCodeKeys.E003);
        ScheduleInfo item = scheduleInfoMap.get(nodeId);
        if (null == item)
        {
            item = new ScheduleInfo(nodeId);
            scheduleInfoMap.put(nodeId, item);
        }
        else
        {
            rc = ReturnCodeKeys.E005;
        }
        return rc;
    }

    public int unregisterNode(int nodeId)
    {
        int rc = (nodeId <= 0 ? ReturnCodeKeys.E004 : ReturnCodeKeys.E006);
        ScheduleInfo item = scheduleInfoMap.get(nodeId);
        rc = (null == item ? ReturnCodeKeys.E007 : rc);
        if (ReturnCodeKeys.E006 == rc) {
            //放入挂起队列
            taskHangSet.addAll(item.getTaskIdSet());
            scheduleInfoMap.remove(nodeId);
        }
        return rc;
    }

    public int addTask(int taskId, int consumption)
    {
        int rc = (taskId <= 0 ? ReturnCodeKeys.E009 : ReturnCodeKeys.E008);
        rc = (checkHasTask(taskId) ? ReturnCodeKeys.E010 : rc);
        if (ReturnCodeKeys.E008 == rc)
        {
            taskHangSet.add(taskId);
            taskInfoMap.put(taskId, new TaskInfoExt(taskId, consumption));

        }
        return rc;
    }

    public int deleteTask(int taskId)
    {
        int rc = (taskId <= 0 ? ReturnCodeKeys.E009 : ReturnCodeKeys.E011);
        rc = (checkHasTask(taskId) ? rc : ReturnCodeKeys.E012);
        if (ReturnCodeKeys.E011 == rc)
        {
            removeTask(taskId);
        }
        return rc;
    }

    public int scheduleTask(int threshold)
    {
        int rc = (threshold <= 0 ? ReturnCodeKeys.E002 : ReturnCodeKeys.E013);
        ScheduleTask scheduleTask = new ScheduleTask();
        scheduleTask.init(scheduleInfoMap, taskInfoMap, taskHangSet, threshold);
        rc = (0 == scheduleTask.scheduleTask() ? ReturnCodeKeys.E014 : rc);
        return rc;
    }

    public int queryTaskStatus(List<TaskInfo> tasks)
    {
        for (Map.Entry<Integer, ScheduleInfo> entry : scheduleInfoMap.entrySet())
        {
            Integer nodeId = entry.getKey();
            Set<Integer> taskIdSet = entry.getValue().getTaskIdSet();
            for (Integer taskId : taskIdSet)
            {
                TaskInfo task = new TaskInfo();
                task.setTaskId(taskId);
                task.setNodeId(nodeId);
                tasks.add(task);
            }
        }
        System.out.print(tasks);

        Collections.sort(tasks, new Comparator<TaskInfo>() {
            public int compare(TaskInfo task1, TaskInfo task2) {
                return  task1.getTaskId() - task2.getTaskId();
            }
        });
        int rc = (tasks.size() > 0 ? ReturnCodeKeys.E015 : ReturnCodeKeys.E016);
        return rc;
    }

    /**
     * 判断任务是否已经被添加
     * @param taskId
     * @return
     */
    private boolean checkHasTask(Integer taskId)
    {
        return  (null != taskInfoMap.get(taskId) ? true : false);
    }

    /**
     * 删除任务
     * @param taskId
     */
    private void removeTask(Integer taskId)
    {
        taskHangSet.remove(taskId);
        TaskInfoExt taskInfoExt = taskInfoMap.get(taskId);
        Integer nodeId = (null != taskInfoExt ? taskInfoExt.getNodeId() : null);
        if (null != nodeId)
        {
            ScheduleInfo item= scheduleInfoMap.get(nodeId);
            if (null != item)
            {
                item.getTaskIdSet().remove(taskId);
            }
        }
        taskInfoMap.remove(taskId);
    }
}
