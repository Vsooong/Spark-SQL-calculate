#!/usr/bin/env python
# coding: utf-8
import subprocess
cmd='./run.sh spark_compute.py'
res = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE, close_fds=True)
while res.poll() is None:
  #每次获取一行输出，并且将byte转换为utf-8格式
  print(res.stdout.readline().decode('utf-8'))
res.wait()
print('Computing job complete!')

def get_result_file():
    !hadoop fs -getmerge dc2019/qes_1.csv /home/liaoshanhe/dc2019/qes1.csv
    !hadoop fs -getmerge dc2019/qes_2.csv /home/liaoshanhe/dc2019/qes2.csv
    !hadoop fs -getmerge dc2019/qes_3.csv /home/liaoshanhe/dc2019/qes3.csv
    def _add_header(path,header):
        with open(path,'r+') as f:
            content=f.read()
            f.seek(0,0)
            f.write(header)
            f.write(content)
    _add_header('/home/liaoshanhe/dc2019/qes1.csv','calling_nbr,avg_calling\n')
    _add_header('/home/liaoshanhe/dc2019/qes2.csv','call_type,called_optr,percent\n')
    _add_header('/home/liaoshanhe/dc2019/qes3.csv',','.join(['calling_nbr', 'period_1', 'period_2', 'period_3', 'period_4', 'period_5', 'period_6', 'period_7', 'period_8'])+'\n')

get_result_file()