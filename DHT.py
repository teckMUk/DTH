import socket 
import threading
import os
import time
import hashlib


class Node:
	def __init__(self, host, port):
		self.stop = False
		self.host = host
		self.port = port
		self.M = 16
		self.N = 2**self.M
		self.key = self.hasher(host+str(port))
		# You will need to kill this thread when leaving, to do so just set self.stop = True
		threading.Thread(target = self.listener).start()
		self.files = []
		self.backUpFiles = []
		if not os.path.exists(host+"_"+str(port)):
			os.mkdir(host+"_"+str(port))
		'''
		------------------------------------------------------------------------------------
		DO NOT EDIT ANYTHING ABOVE THIS LINE
		back of the files will be stored in the succesor node if the node fails so the succesor node will have the file with it self
		'''
		# Set value of the following variables appropriately to pass Intialization test
		self.successor = (self.host,self.port)
		self.predecessor = (self.host,self.port)
		self.next_successor = (self.host,self.port)
		self.ping_stop = False
		# additional state variables



	def hasher(self, key):
		'''
		DO NOT EDIT THIS FUNCTION.
		You can use this function as follow:
			For a node: self.hasher(node.host+str(node.port))
			For a file: self.hasher(file)
		'''
		return int(hashlib.md5(key.encode()).hexdigest(), 16) % self.N


	def handleConnection(self, client, addr):
		'''
		 Function to handle each inbound connection, called as a thread from the listener.
		'''
		msg = client.recv(1024).decode("utf-8")
		msg_split = msg.split()
		if msg_split[0] == "get_sucessor":
			msg_sent = self.successor[0] + " " + str(self.successor[1])
			client.send(msg_sent.encode('utf-8'))
		elif msg_split[0] == "update":
			self.successor = (msg_split[1],int(msg_split[2]))
			self.predecessor =(msg_split[1],int(msg_split[2]))
		elif msg_split[0] == "get_predecessor":
			msg_sent = self.predecessor[0] + " " + str(self.predecessor[1])
			client.send(msg_sent.encode('utf-8'))
		elif msg_split[0] == "update_predecessor":
			self.predecessor = (msg_split[1],int(msg_split[2]))
		elif msg_split[0] == "update_sucessor":
			self.successor = (msg_split[1],int(msg_split[2]))
			self.next_successor = self.get_sucessor(self.successor)
		elif msg_split[0] == "update_predecessor_ping":
			self.predecessor = (msg_split[1],int(msg_split[2]))
		elif msg_split[0] == "recive_file":
			self.files.append(msg_split[1])
			file_path = self.host+"_"+str(self.port) +"/" + msg_split[1]
			self.recieveFile(client,file_path)
			sock = socket.socket()
			sock.connect(self.successor)
			msg_sent = "recive_file_back_up " + msg_split[1]
			sock.send(msg_sent.encode('utf-8'))
			time.sleep(0.5)
		elif msg_split[0] == "sent_file":
			if msg_split[1] in self.files:
				msg = "yes"
				client.send(msg.encode('utf-8'))
				file_path = self.host+"_"+str(self.port) +"/" + msg_split[1]
				time.sleep(0.5)
				self.sendFile(client,file_path)
			else:
				msg = "No"
				client.send(msg.encode('utf-8'))
		elif msg_split[0] == "sent_rehash_file":
			file_to_be_sent_list = []
			list_of_files = self.files
			for file_x in list_of_files:
				if self.key > int(msg_split[1]):
					if self.hasher(file_x) <= int(msg_split[1]):
						file_to_be_sent_list.append(file_x)
				else:
					if (self.hasher(file_x)> self.key) and (self.hasher(file_x)<=int(msg_split[1])):
						file_to_be_sent_list.append(file_x)
			self.backUpFiles = file_to_be_sent_list
			for file_y in file_to_be_sent_list:
				self.files.remove(file_y)
			sock = socket.socket()
			sock.connect(self.successor)
			msg_sent = "Do_back_up " + str(len(self.files))
			sock.send(msg_sent.encode('utf-8'))
			time.sleep(0.5)
			for file_z in self.files:
				msg_sent = file_z
				sock.send(msg_sent.encode('utf-8'))
				time.sleep(0.2)
			msg = str(len(file_to_be_sent_list))
			client.send(msg.encode('utf-8'))
			for file_y in file_to_be_sent_list:
				file_path = self.host+"_"+str(self.port) +"/" + file_y
				msg = file_y
				client.send(msg.encode('utf-8'))
				time.sleep(0.5)
				self.sendFile(client,file_path)
				time.sleep(0.2)
		elif msg_split[0]=="recive_file_back_up":
			self.backUpFiles.append(msg_split[1])
			
		elif msg_split[0]=='Do_back_up':
			self.backUpFiles.clear()
			for i in range(int(msg_split[1])):
				msg_rec = client.recv(1024).decode('utf-8')
				self.backUpFiles.append(msg_rec)
    
		elif msg_split[0] == "update_backup":
			self.backUpFiles.clear()
			sock = socket.socket()
			sock.connect(self.predecessor)
			msg = "sent_all_files"
			sock.send(msg.encode('utf-8'))
			rec_msg = sock.recv(1024).decode('utf-8')
			for i in range(int(rec_msg)):
				rec_msg = sock.recv(1024).decode('utf-8')
				self.backUpFiles.append(rec_msg)
				
				
				
		elif msg_split[0] == "sent_all_files":
			msg = str(len(self.files))
			client.send(msg.encode('utf-8'))
			time.sleep(0.2)
			for file_x in self.files:
				msg = file_x
				client.send(msg.encode('utf-8'))
				time.sleep(0.2)
    
		elif msg_split[0]== "move_backup_to_file":
			for file_x in self.backUpFiles:
				self.files.append(file_x)
		elif msg_split[0] == "recive_file_add":
			self.files.append(msg_split[1])
			sock = socket.socket()
			sock.connect(self.successor)
			msg_sent = "recive_file_back_up " + msg_split[1]
			sock.send(msg_sent.encode('utf-8'))
			time.sleep(0.2)
				

			
				

	def listener(self):
		'''
		We have already created a listener for you, any connection made by other nodes will be accepted here.
		For every inbound connection we spin a new thread in the form of handleConnection function. You do not need
		to edit this function. If needed you can edit signature of handleConnection function, but nothing more.
		'''
		listener = socket.socket()
		listener.bind((self.host, self.port))
		listener.listen(10)
		while not self.stop:
			client, addr = listener.accept()
			threading.Thread(target = self.handleConnection, args = (client, addr)).start()
		print ("Shutting down node:", self.host, self.port)
		try:
			listener.shutdown(2)
			listener.close()
		except:
			listener.close()

	def join(self, joiningAddr):
		'''
		This function handles the logic of a node joining. This function should do a lot of things such as:
		Update successor, predecessor, getting files, back up files. SEE MANUAL FOR DETAILS.
		'''	
		if joiningAddr!="":
			my_successor = self.get_sucessor(joiningAddr)
			if my_successor==joiningAddr:
				self.successor = joiningAddr
				self.predecessor = joiningAddr

				sock = socket.socket()
				sock.connect(joiningAddr)
				msg = "update " + self.host + " " + str(self.port)
				sock.send(msg.encode('utf-8'))
			else:
				my_successor = self.look_up(joiningAddr,self.key)
				#print(joiningAddr)
				#print(my_successor)
				self.successor = my_successor
				sock = socket.socket()
				sock.connect(self.successor)
				msg_sent = "update_predecessor" + " " + self.host + " " + str(self.port)
				sock.send(msg_sent.encode('utf-8'))
				sock.close()
				self.next_successor = self.get_sucessor(my_successor)
			self.rehash_file(my_successor)
		threading.Thread(target = self.pinging).start()



	def put(self, fileName):
		'''
		This function should first find node responsible for the file given by fileName, then send the file over the socket to that node
		Responsible node should then replicate the file on appropriate node. SEE MANUAL FOR DETAILS. Responsible node should save the files
		in directory given by host_port e.g. "localhost_20007/file.py".
		'''
		file_key = self.hasher(fileName)	
		find_node = self.look_up((self.host,self.port),file_key)
		sock = socket.socket()
		sock.connect(find_node)
		msg_sent = "recive_file " + fileName
		sock.send(msg_sent.encode('utf-8'))
		time.sleep(0.5)
		self.sendFile(sock,fileName)
		sock.close()
		
		
	def get(self, fileName):
		'''
		This function finds node responsible for file given by fileName, gets the file from responsible node, saves it in current directory
		i.e. "./file.py" and returns the name of file. If the file is not present on the network, return None.
		'''
		file_key = self.hasher(fileName)	
		find_node = self.look_up((self.host,self.port),file_key)
		sock = socket.socket()
		sock.connect(find_node)
		msg_sent = "sent_file " + fileName
		sock.send(msg_sent.encode('utf-8'))
		time.sleep(0.5)
		file_path = "./"+ fileName
		recv_msg = sock.recv(1024).decode('utf-8')
		if recv_msg=="yes":
			self.recieveFile(sock,file_path)
			sock.close()
			return fileName
		else:
			sock.close()
			return None

		
	def leave(self):
		'''
		When called leave, a node should gracefully leave the network i.e. it should update its predecessor that it is leaving
		it should send its share of file to the new responsible node, close all the threads and leave. You can close listener thread
		by setting self.stop flag to True
		'''
		self.ping_stop = True
		#sent msg to my succesor to update it predecessor with my predecessor
		sock = socket.socket()
		sock.connect(self.successor)
		msg = "update_predecessor " + self.predecessor[0] +" "+ str(self.predecessor[1])
		sock.send(msg.encode('utf-8'))
		sock.close()
		# sent msg to my predecessor to updates it successor with my successor
		sock = socket.socket()
		sock.connect(self.predecessor)
		msg = "update_sucessor " + self.successor[0] + " " +str(self.successor[1])
		sock.send(msg.encode('utf-8'))
		sock.close()
		#sent all my files to my successor
		for file_x in self.files:
			sock = socket.socket()
			sock.connect(self.successor)
			msg_sent = "recive_file_add " + file_x
			sock.send(msg_sent.encode('utf-8'))
			time.sleep(0.2)
			sock.close()
		sock = socket.socket()
		sock.connect(self.successor)
		msg = "update_backup " 
		sock.send(msg.encode('utf-8'))
		time.sleep(5)
		sock.close()
		self.kill()

	def sendFile(self, soc, fileName):
		''' 
		Utility function to send a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		fileSize = os.path.getsize(fileName)
		soc.send(str(fileSize).encode('utf-8'))
		soc.recv(1024).decode('utf-8')
		with open(fileName, "rb") as file:
			contentChunk = file.read(1024)
			while contentChunk!="".encode('utf-8'):
				soc.send(contentChunk)
				contentChunk = file.read(1024)

	def recieveFile(self, soc, fileName):
		'''
		Utility function to recieve a file over a socket
			Arguments:	soc => a socket object
						fileName => file's name including its path e.g. NetCen/PA3/file.py
		'''
		fileSize = int(soc.recv(1024).decode('utf-8'))
		soc.send("ok".encode('utf-8'))
		contentRecieved = 0
		file = open(fileName, "wb")
		while contentRecieved < fileSize:
			contentChunk = soc.recv(1024)
			contentRecieved += len(contentChunk)
			file.write(contentChunk)
		file.close()
  
	def look_up(self,joiningAddr,key_id):
		mysuccessor = self.get_sucessor(joiningAddr)
		my_id = self.hasher(joiningAddr[0]+str(joiningAddr[1]))
		n = self.hasher(mysuccessor[0]+str(mysuccessor[1]))
		if n>my_id:
			if my_id<=key_id<n:
				return mysuccessor
			else:
				return self.look_up(mysuccessor,key_id)
		else:
			if key_id<=n or key_id>my_id:
				return mysuccessor
			else:
				return self.look_up(mysuccessor,key_id)

	def get_sucessor(self,joiningAddr):
		sock = socket.socket()
		sock.connect(joiningAddr)
		msg = "get_sucessor"
		sock.send(msg.encode('utf-8'))
		msg_recvide = sock.recv(1024).decode('utf-8')
		sock.close()
		msg_split = msg_recvide.split()
		my_successor = (msg_split[0],int(msg_split[1]))
		#print("get",my_successor)
		return my_successor

	def get_predecessor(self,joiningAddr):
		sock = socket.socket()
		sock.connect(joiningAddr)
		msg = "get_predecessor"
		sock.send(msg.encode('utf-8'))
		msg_recvide = sock.recv(1024).decode('utf-8')
		sock.close()
		msg_split = msg_recvide.split()
		my_predecessor = (msg_split[0],int(msg_split[1]))
		return my_predecessor

	def pinging(self):
		i = 0
		while self.ping_stop==False:
			if self.stop==True:
				break
			if i >=3:
				self.successor = self.next_successor
				sock = socket.socket()
				sock.connect(self.successor)
				msg = "update_predecessor " + self.host +" "+ str(self.port)
				sock.send(msg.encode('utf-8'))
				sock.close()
				self.next_successor = self.get_sucessor(self.successor)
				sock = socket.socket()
				sock.connect(self.successor)
				msg = "move_backup_to_file "
				sock.send(msg.encode('utf-8'))
				sock.close()
				sock = socket.socket()
				sock.connect(self.successor)
				msg = "update_backup " 
				sock.send(msg.encode('utf-8'))
				sock.close()
    
				i =0
			else:		
				sock = socket.socket()
				try:
					sock.connect(self.successor)
				except socket.error:
					i+=1
					sock.close()
					continue
				msg_sent = "get_predecessor"
				sock.send(msg_sent.encode('utf-8'))
				sock.settimeout(1)
				try:
					msg = sock.recv(1024).decode('utf-8')
				except socket.timeout:
					i+=1
					sock.close()
					continue
				
				msg_split = msg.split()
				get_predecessor = (msg_split[0],int(msg_split[1]))		
				i = 0
				if get_predecessor != (self.host,self.port):
					self.next_successor = self.successor
					self.successor = get_predecessor
					sock = socket.socket()
					try: 
						sock.connect(self.successor)
					except socket.error:
						i+=1
						sock.close()
						continue
					msg_sent = "update_predecessor_ping" + " " + self.host +" "+ str(self.port)
					sock.send(msg_sent.encode('utf-8'))
					sock.close()
					sock = socket.socket()
					sock.connect(self.successor)
					msg = "update_backup " 
					sock.send(msg.encode('utf-8'))
					time.sleep(5)
					sock.close()
					

    
	def rehash_file(self,my_successor):
		sock = socket.socket()
		sock.connect(my_successor)
		msg = "sent_rehash_file " + str(self.key)
		sock.send(msg.encode('utf-8'))
		msg_recv = sock.recv(1024).decode('utf-8')
		for i in range(int(msg_recv)):	
			msg_recv = sock.recv(1024).decode('utf-8')
			self.files.append(msg_recv)
			file_path = file_path = self.host+"_"+str(self.port) +"/" + msg_recv
			self.recieveFile(sock,file_path)

		
         
	def kill(self):
		# DO NOT EDIT THIS, used for code testing
		self.stop = True
  
     
     

		
