import paramiko
import time
import socket
import sys
import argparse
from dynamic_mapper import fetch_from_db
from paramiko.client import RejectPolicy
from paramiko.ssh_exception import AuthenticationException, NoValidConnectionsError, BadHostKeyException, SSHException



def check(**kwargs):
    #list1 = ["10.202.96.191","10.202.96.192"]
    print file_name
    with open(file_name,"r") as text:
    	list1 = [line.strip('\n') for line in text]
    errorip = []
    username = kwargs.get('username', False)
	password = kwargs.get('password', False)
    password = 'JAxa466'
    session = paramiko.SSHClient()
    session.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    for xaddr in list1:
        print "now connecting to: "+xaddr
        try:
            session.connect(xaddr, username=username, password=password, look_for_keys=False, allow_agent=False)
        except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
	    print "SSH ERROR"
	    continue
        # connection.recv(1000)
        connection = session.invoke_shell()
        connection.send("terminal length 0")
        connection.send("\n")
        """
	message1 = "configure terminal"
        connection.send(message1 +"\n")
        #connection.send("\n")
        time.sleep(1.0)
	message2 = "no ip access-list standard SNMP_ACL"
        connection.send(message2 +"\n")
        #connection.send("\n")
        time.sleep(1.0)
	"""
	with open("commands.txt","r") as cmd:
    		commands = [line.strip('\n') for line in cmd]

	"""
	commands =["ip access-list standard SNMP_ACL","remark Version_Q12018",
 "permit 10.132.8.0 0.0.3.255",
 "permit 10.133.176.0 0.0.2.255",
 "permit 10.134.168.0 0.0.3.255",
 "permit 10.134.240.0 0.0.3.255",
 "permit 10.139.84.0 0.0.1.255",
 "permit 10.186.4.0 0.0.0.15",
 "permit 10.186.203.0 0.0.0.15",
 "permit 10.194.102.0 0.0.1.255",
 "permit 10.194.236.0 0.0.3.255",
 "permit 10.194.92.0 0.0.3.255",
 "permit 10.215.137.0 0.0.0.127",
 "permit 10.134.179.0 0.0.0.128**",
 "deny   any",
 "exit"]
	"""
	for command in commands:
		print "sending :", command
		connection.send(command +"\n")
		time.sleep(0.2)
		data = connection.recv(1000)
		if data.find("Invalid") == -1:
		    continue
		else:
		    print "got invalid error while executing:",command
		    errorip.append(xaddr)
		    #sys.exit("invalid")
		    continue
		
	print errorip
        """
	output_intermediate = connection.recv(164535)
	print output_intermediate
	text2 = [line.strip('\n') for line in output_intermediate]
        f = open("configuration.txt", "w+")
        for line in text2:
           f.write(line)
           f.write("\n")
	"""
	
        session.close()
        #f.close()

parser = argparse.ArgumentParser()
parser.add_argument('-k', action='store', dest='file_name', help='pass the text_name')
result = parser.parse_args()
if result.file_name:
    check(result.file_name)
else: 
    print "pass text file with -k flag"

