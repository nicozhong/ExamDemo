package example;

import static org.junit.Assert.*;

import com.migu.schedule.info.ScheduleInfo;
import com.migu.schedule.info.TaskInfoExt;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

public class OneTest {
  @Test
  public void testFoo() throws Exception {
    One one = new One();
    //Test foo
    assertEquals("foo", one.foo());
  }

  @Test
  public void testLinkedHashMap()
  {
    Map<Integer/*index*/, Integer/*taskId*/> taskIdIndexMap = new LinkedHashMap<Integer, Integer>();
    taskIdIndexMap.put(1, 5);
    taskIdIndexMap.put(3,3);
    taskIdIndexMap.put(2,3);
    System.out.println(taskIdIndexMap);
  }

  @Test
  public void testTaskInfoExt()
  {
    TaskInfoExt taskInfoExt = new TaskInfoExt();
    taskInfoExt.setNodeId(1);
    taskInfoExt.setConsumption(10);
    taskInfoExt.getTaskInfo().setTaskId(2);
    TaskInfoExt taskInfoExt2 = new TaskInfoExt();
    taskInfoExt2 = (TaskInfoExt)taskInfoExt.clone();
    taskInfoExt.getTaskInfo().setTaskId(22);
    System.out.println("taskInfoExt:\n" + taskInfoExt);
    System.out.println("taskInfoExt2:\n" + taskInfoExt2);
  }
  @Test
  public void testScheduleInfo()
  {
    ScheduleInfo item = new ScheduleInfo();
    item.setSumConsumption(100);
    item.getTaskIdSet().add(1);
    item.getTaskIdSet().add(2);
    item.initTaskIdIndexMap();
    ScheduleInfo citem = (ScheduleInfo)item.clone();
    item.removeTaskId(1);
    System.out.println("item:\n" + item);
    System.out.println("citem:\n" + citem);
  }
}