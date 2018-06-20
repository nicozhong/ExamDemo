package com.migu.schedule.info;

public class TaskInfoExt implements Cloneable{
    /**
     * 任务信息
     */
    private TaskInfo taskInfo = new TaskInfo();
    /**
     * 资源消耗率
     */
    private Integer consumption;

    public TaskInfoExt()
    {
    }

    public TaskInfoExt(Integer taskId, Integer consumption)
    {
        taskInfo.setTaskId(taskId);
        taskInfo.setNodeId(-1);
        this.consumption = consumption;
    }

    public Integer getConsumption() {
        return consumption;
    }

    public void setConsumption(Integer consumption) {
        this.consumption = consumption;
    }

    public Integer getNodeId() {
        return taskInfo.getNodeId();
    }

    public void setNodeId(Integer nodeId) {
        taskInfo.setNodeId(nodeId);
    }

    public TaskInfo getTaskInfo() {
        return taskInfo;
    }

    public void setTaskInfo(TaskInfo taskInfo) {
        this.taskInfo = taskInfo;
    }

    @Override
    public Object clone() {
        TaskInfoExt item = new TaskInfoExt();
        item.setConsumption(consumption);
        TaskInfo cTaskInfo = new TaskInfo();
        cTaskInfo.setTaskId(taskInfo.getTaskId());
        cTaskInfo.setNodeId(taskInfo.getNodeId());
        item.setTaskInfo(cTaskInfo);
        return item;
    }

    public String toString()
    {
        StringBuilder str = new StringBuilder();
        str.append("taskId:").append(taskInfo.getTaskId())
                .append(", nodeId:").append(taskInfo.getNodeId())
                .append(", consumption:").append(consumption);
        return str.toString();
    }
}
