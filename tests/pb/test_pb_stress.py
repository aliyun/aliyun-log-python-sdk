import os
import unittest

import time
import aliyun_log_pb as slspb
import six
import concurrent.futures

def _gen_varint(value):
    pieces = []
    bits = value & 0x7f
    value >>= 7

    while value:
        pieces.append(six.int2byte(0x80|bits))
        bits = value & 0x7f
        value >>= 7
    pieces.append(six.int2byte(bits))
    out = pieces[0]

    for i in range(1,len(pieces)):
        out += pieces[i]
    return out

def run_stress_test():
    threadpool = ThreadPool()
    threadpool.start_task()

class ThreadPool():
  def __init__(self) -> None:
    pass

  def start_task(self):
      with concurrent.futures.ThreadPoolExecutor() as executor:
          futures = [executor.submit(self.stress_test) for i in range(10)]
          # 获取任务的结果
          for  index, future in enumerate(concurrent.futures.as_completed(futures)):
            try:
              # 调用result()方法获取任务的返回值
              result = future.result()
            except Exception as e:
              # 处理任务异常
              print(f"任务执行发生异常{index}：{e}") 

  def stress_test(self):
      base_log = [{'__topic__': '', '__source__': '128.9.9.1', '__time__': '1597317524', '__time_ns_part__': '701068179', 'a': 'b', '__tag__:4': '5', '__tag__:6': '7'}, {'__topic__': '', '__source__': '128.9.9.1', '__time__': '1701068179', '__time_ns_part__': '597317524', 'b': 'a', '__tag__:4': '5', '__tag__:6': '7'}]
      log = [0,None,'128.9.9.1',[("4","5"),("6","7")],[1597317524,1701068179],[701068179,597317524],[[("a","b")],[("b","a")]]]
      start_time = time.time()
      loop_count = 0
      run_duration_hours = 24
      while(True):
          loop_count +=1
          (encoded_log, raw_size, skip_cnt, warnings) = slspb.write_pb(log)
          loggroup_list_pb_str = bytes.fromhex('0a') + _gen_varint(len(encoded_log)) + encoded_log
          (decoded_log, _) = slspb.parse_pb([loggroup_list_pb_str,1])
          # 验证解码的结果与原始数据一致
          assert decoded_log == base_log
          elapsed_time_hours = (time.time() - start_time) / 3600
          if (time.time() - start_time)%3600 == 0:
              print(f"Stress test progress:")
              print(f"  Iterations: {loop_count}")
              print(f"  Elapsed time: {elapsed_time_hours} hours")

          # 如果运行时长达到指定时长，跳出循环
          if elapsed_time_hours >= run_duration_hours:
              break
      elapsed_time = time.time() - start_time
      print("Stress test completed:")
      print("  Iterations:", loop_count)
      print("  Elapsed time:", elapsed_time, "seconds")

class TestWritePB(unittest.TestCase):

    def test_slspb_writepb(self):
        run_stress_test()


if __name__ == '__main__':
    unittest.main()
