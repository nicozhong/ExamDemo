package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.util.List;

/*
*类名和方法不能修改
 */
public class Schedule {

    /**
     * 任务管理器
     */
    private NodeTaskManage nodeTaskManage = new NodeTaskManage();

    public int init() {
        return nodeTaskManage.init();
    }


    public int registerNode(int nodeId) {
        return nodeTaskManage.registerNode(nodeId);
    }

    public int unregisterNode(int nodeId) {
        return nodeTaskManage.unregisterNode(nodeId);
    }


    public int addTask(int taskId, int consumption) {
        return nodeTaskManage.addTask(taskId, consumption);
    }


    public int deleteTask(int taskId) {
        return nodeTaskManage.deleteTask(taskId);
    }


    public int scheduleTask(int threshold) {
        return nodeTaskManage.scheduleTask(threshold);
    }


    public int queryTaskStatus(List<TaskInfo> tasks) {
        return nodeTaskManage.queryTaskStatus(tasks);
    }

}
