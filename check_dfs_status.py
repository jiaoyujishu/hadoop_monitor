#!/usr/bin/env python
import sys
import os
import re
import json
import requests
from bs4 import BeautifulSoup

def GetValue(url):
	r = requests.get(url)
	html = r.content
        return html
def CheckData(html):
 	Heap_mem = re.search(".+Heap Memory used.+</div><div>",html)
	Heap = re.split('<div>|</div>|<b>',Heap_mem.group())
	Heap_used,Heap_persent,Heap_commited = (Heap[4].split()[3],Heap[4].split()[6],Heap[4].split()[11])
	Dfs_msg = re.findall('.+Configured Capacity.+',html)
	Dfs_msg = re.split('<td id="col\d+">|:|</?div>|<tr class="\w+">|<br>|"\ "|</table>',Dfs_msg[0])
	Dfs_info = [value for value in Dfs_msg if value != "" and value != " "]
	Dfs_total = Dfs_info[1]
	Dfs_used = Dfs_info[3]
	Non_Dfs_used = Dfs_info[5]
	Dfs_Remaining = Dfs_info[7]
	Dfs_used_precent = Dfs_info[9]
	Node_live = Dfs_info[-7].split()[0].strip()
	Node_dead = Dfs_info[-4].split()[0].strip()
	Node_Decomm = Dfs_info[-1].strip()
	hadoop_msg = "Heap_used:%s,Heap_commited:%s,Heap_persent,%s,Dfs_total:%s,Dfs_used:%s,Non_Dfs_used:%s,Dfs_Remaining:%s,Dfs_used_precent:%s,Node_live:%s,Node_dead:%s,Node_Decomm:%s" %(Heap_used,Heap_commited,Heap_persent,Dfs_total,Dfs_used,Non_Dfs_used,Dfs_Remaining,Dfs_used_precent,Node_live,Node_dead,Node_Decomm)
	return hadoop_msg

def CheckJob(html):
	content=[]
	soup = BeautifulSoup(html)
	table_cluster = soup.findAll('table',attrs={'id':'metricsoverview'})
	table_user = soup.findAll('table',attrs={'id':'usermetricsoverview'})
	list_cluster,list_user = [],[]
	for row in table_cluster[0].findAll('tr'):
		for tr in row.findAll('td'):
			list_cluster.append(tr.get_text().encode("utf-8").strip())
	for row in table_user[0].findAll('tr'):
		for tr in row.findAll('td'):
			list_user.append(tr.get_text().encode("utf-8").strip())
	job_cluster = "Appsubmit:%s,Apppending:%s,Apprunning:%s,Appcompleted:%s,Containerrunning:%s,Memused:%s,Memtotal:%s,Vcoreused:%s,Vcoretotal:%s,activenodes:%s,Lostnodes:%s,Unhealthynoded:%s,RebootNodes:%s"%(list_cluster[0],list_cluster[1],list_cluster[2],list_cluster[3],list_cluster[4],list_cluster[5],list_cluster[6],list_cluster[8],list_cluster[9],list_cluster[11],list_cluster[12],list_cluster[13],list_cluster[14])
	#print list_user
	return job_cluster

def SenData(value):
	pass


if __name__ == '__main__':
	#url = "http://10.49.102.16:8080/dfshealth.jsp"
	hdfs_url = "http://10.49.101.90:50070/dfshealth.jsp"
	job_url = "http://10.175.128.27:8080/cluster/cluster"
	html = GetValue(hdfs_url)
	print CheckData(html)
	html = GetValue(job_url)
	print CheckJob(html)
