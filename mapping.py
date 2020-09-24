from exceptions import Error
import multiprocessing
import os
from retrying import retry
import time
import threading

class Mapping(Error):

	def __init__(self, n_files=5, n_maps=0, bug=False):
		self.n_files = n_files
		self.n_maps = n_maps
		self.file_to_map = "map_file.txt"
		self.bug = bug

	def assignThreads(self):
		distributor = [""] * self.n_maps

		current_file = 1
		current_map = 1

		while current_file < self.n_files + 1:

			distributor[current_map - 1] += str(current_file) + " "

			current_file += 1
			current_map += 1

			if current_map == self.n_maps +1:
				current_map = 1

		while ("" in distributor):
			distributor.remove("")

		return(distributor)


	@retry
	def mapper(self, text_file):
		map_file = open(self.file_to_map, "+a")

		key_value = {}

		while True:
			if self.bug:
				self.setBug(False)
				self.raiseExeption("Error: En mapa")

			line = text_file.readline().lower()

			if not line:
				break

			keys = line.strip().split(" ")

			for word in keys:
				if word == "":
					continue

				if word in key_value:
					key_value[word] += 1

				else:
					key_value[word] = 1

		for key in key_value:
			map_file.write(str(key) + ":" + str(key_value[key]) + "\n")

		map_file.close()

	def map_task(self, file_array):
		for file in file_array:
			text_file = open(str(file) + ".txt", "r")

			self.mapper(text_file)

			print("file:" + str(file) + ".txt \t OK" )
			text_file.close()
	
	def start(self):

		if os.path.exists(self.file_to_map):
			os.remove(self.file_to_map)

		n_files = self.assignThreads()

		processes = []

		for file in n_files:
			file_array = file.strip().split(" ")
			#p = threading.Thread(target= self.map_task, args=(file_array))
			p = multiprocessing.Process(target= self.map_task, args=[file_array])
			p.start()
			processes.append(p)

		for process in processes:
			process.join()

		print("Tarea 2: Mapping Completado")

	def getTaskCompleted(self):
		return self.taskCompleted

	def setBug(self,bug):
		self.bug = bug

	def setNFiles(self, n):
		self.n_files = n

	def setNMaps(self, n):
		self.n_maps = n


