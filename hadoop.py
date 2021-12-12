import subprocess as sb
from time import sleep
import os

# for Configuring files
def file_handeling(file_path, ip, namenode):
	file = open("{}".format(file_path), 'r')
	string_list = file.readlines()
	file.close()

	index_initial = string_list.index('<configuration>\n')
	index_final = string_list.index('</configuration>\n')

	del string_list[index_initial+1:index_final]
	if file_path == '/etc/hadoop/hdfs-site.xml':
		string_list.insert(index_initial + 1, "<property>\n")
		if namenode == True:
			string_list.insert(index_initial + 2, "<name>dfs.name.dir</name>\n")
		else:
			string_list.insert(index_initial + 2, "<name>dfs.data.dir</name>\n")
		string_list.insert(index_initial + 3, "<value>/nn</value>\n")
		string_list.insert(index_initial + 4, "</property>\n")
	elif file_path == '/etc/hadoop/core-site.xml':
		string_list.insert(index_initial + 1, "<property>\n")
		string_list.insert(index_initial + 2, "<name>fs.default.name</name>\n")
		string_list.insert(index_initial + 3, "<value>hdfs://{}:9001</value>\n".format(ip))
		string_list.insert(index_initial + 4, "</property>\n")
	else: sb.call('echo "configuration file not found"', shell=True)

	file = open("{}".format(file_path), "w")
	new_file_content = "".join(string_list)
	file.write(new_file_content)
	file.close()
	
def configure_namenode_hadoop(Type):
	# if Type == 1:
	# 	ip = '192.168.43.194'
	#else:
	ip = '0.0.0.0'
	sleep(1)
	os.system('tput setaf 3')
	sb.call("echo '[Namenode]'", shell=True)
	sb.call("echo 'Configuring hdfs-site.xml file...'", shell=True)

	file_handeling('/etc/hadoop/hdfs-site.xml', '0.0.0.0', True)
	sleep(1)
	sb.call("echo 'ConfiguredConfigured hdfs-site.xml file...'", shell=True)
	sleep(1)
	sb.call("echo 'Configuring core-site.xml file...'", shell=True)

	file_handeling('/etc/hadoop/core-site.xml', ip, True)
	sleep(1)
	sb.call("echo 'Configured core-site.xml file...'", shell=True)
	sleep(1)
	sb.call("echo 'Formatting Namenode...'", shell=True)
	sb.call("echo 'Y' | hadoop namenode -format",shell = True)
	# if out[0] == 0:
		
	# 	sb.call("echo 'Namenode successfully fomatted !'", shell=True)
	# else:
	# 	os.system('tput setaf 1') 
	# 	print('Something went Wrong while formatting !')
	# 	print('Trying again..')
	# 	sb.getstatusoutput("echo 'Y' | hadoop namenode -format")
	sb.call("echo 'Namenode successfully fomatted !'", shell=True)
	sleep(1)
	os.system('tput setaf 3')
	sb.call("echo 'Starting Namenode...'", shell=True)
	sb.call("echo 3 > /proc/sys/vm/drop_caches", shell=True)
	out = sb.getstatusoutput("hadoop-daemon.sh start namenode")
	
	if 'running' in out[1]:
		sb.getstatusoutput("hadoop-daemon.sh stop namenode")
		out = sb.getstatusoutput("hadoop-daemon.sh start namenode")
	if out[0] == 0:
		os.system('tput setaf 2')
		sb.call("echo 'Namenode started successfully !'", shell=True)
	else:
		os.system('tput setaf 1')
		print('Something went Wrong !')
		print(out[1])
		os.system('tput setaf 7')

def configure_datanodes_hadoop(Type, ips):
	for ip in ips:
		sleep(1)
		print("[{}]".format(ip))
		sb.getstatusoutput("sudo chmod 400 /home/ec2-user/DSCWOW.TUI/aws1.pem")
		if Type == 1:
			sb.getoutput('ssh -o StrictHostKeyChecking=No -i /home/ec2-user/DSCWOW.TUI/aws1.pem ec2-user@{} "sudo python3" < /home/ec2-user/DSCWOW.TUI/gitd.py'.format(ip))
			sb.call('ssh -o StrictHostKeyChecking=No -i /home/ec2-user/DSCWOW.TUI/aws1.pem ec2-user@{} "sudo python3 /home/ec2-user/DSCWOW.TUI/datanode.py"'.format(ip), shell=True)

def configure_cluster(ips):
	configure_namenode_hadoop(1)
	configure_datanodes_hadoop(1,ips)
	sb.call("hadoop dfsadmin -report", shell=True)

def hadoop():
	while True:
		os.system('tput setaf 10')
		print("""
			-----------------------------------------------------
				Hadoop:
			-----------------------------------------------------	
				1. Configure Hadoop Namenode
				2. Configure Hadoop Datanode
				3. Configure the Whole Cluster
				4. Show Report
				5. Main Menu
			-----------------------------------------------------
			""")
		os.system("tput setaf 2")
		ch  = ""
		while ch == "":
			ch = input("Enter choice : ")
		ch = int(ch)
		
		os.system('tput setaf 7')
		if ch == 1:
			configure_namenode_hadoop(1)
		elif ch == 2:
			ips = list(input('Enter IPs of Datanodes separated by space : ').split(" "))
			configure_datanodes_hadoop(1, ips)
		elif ch == 3:
			ips = list(input('Enter IPs of Datanodes separated by space : ').split(" "))
			configure_cluster(ips)
		elif ch == 4:
			os.system("hadoop dfsadmin -report")
		elif ch == 5:
			os.system("clear")
			break
		else:
			os.system("tput setaf 1")
			print("Invalid Input!")