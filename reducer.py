from exceptions import Error
import multiprocessing
import os
from retrying import retry
import operator

class Reduce(Error):

	def __init__(self,bug=False):

		self.final_file = "final_file.txt"
		self.shuffle_file = "shuffle_file.txt"
		self.bug = bug

	def reducer(self, l, s, f):
		final_file = open(self.final_file, "+a")
		for i in range(s,f):
			key, values = l[i].split(":")

			values = values.split(",")

			v = 0

			for val in values:
				v += int(val)

			
			final_file.write(str(key) + ":" + str(v) + "\n")
		final_file.close()

	def fileSortedKey(self):
		file = open("final_file.txt","r")

		key_value = {}
		for line in file.readlines():
			k, v = line.strip().split(":")
			key_value[k] = int(v)

		sorted_value = sorted(key_value.items(), key=lambda x: x[1], reverse=True)

		file_value = open("final_file_value.txt", "+a")
		for i in sorted_value:
			file_value.write(str(i[0]) + ":" + str(i[1]) + "\n")
		file_value.close()




	@retry
	def start(self):
		
		if os.path.exists(self.final_file):
			os.remove(self.final_file)

		file = open(self.shuffle_file, "r")

		shuffle_list = file.readlines()

		n_lines = len(shuffle_list)

		part_1 = int(n_lines/2)

		#2 Reducers
		p = multiprocessing.Process(target= self.reducer, args=[shuffle_list, 0, part_1])
		p1 = multiprocessing.Process(target= self.reducer, args=[shuffle_list, part_1 + 1, n_lines])

		p.start()
		p1.start()
		p.join()
		p1.join()
		
		file.close()
		print("Task 4: Reduce Completed")

	def setBug(self, bug):
		self.bug = bug

	def getBug(self, bug):
		return self.bug
