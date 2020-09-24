from exceptions import Error
import os
import collections
from retrying import retry


class Shuffle(Error):

	def __init__(self, bug=False):
		self.map_file = "map_file.txt"
		self.shuffle_file = "shuffle_file.txt"
		self.bug = bug

	@retry
	def start(self):
		if os.path.exists(self.shuffle_file):
			os.remove(self.shuffle_file)

		sorted_file = open(self.shuffle_file, "a+")

		keys_file = open(self.map_file, "r")

		key_value = {}

		while True:

			if self.bug:
				self.setBug(False)
				self.raiseExeption("Error en Shuffle")

			line = keys_file.readline()

			if not line:
				break

			keys = line.strip().split(":")

			try:
				if key_value[keys[0]]:
					key_value[keys[0]].append(keys[1])
			except Exception as e:
				key_value[keys[0]] = [keys[1]]

		key_value = collections.OrderedDict(sorted(key_value.items()))

		for key in key_value:
			sorted_file.write(key + ":" + ",".join(key_value.get(key)) + "\n")

		keys_file.close()
		sorted_file.close()
		print("Task 3: Shuffle Completado")

	def getTaskCompleted(self):
		return self.taskCompleted

	def setBug(self, bug):
		self.bug = bug

	def getBug(self):
		return self.bug

