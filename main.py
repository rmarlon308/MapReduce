from mapping import Mapping
from read_process import ReadDocument
from shuffle import Shuffle
from reducer import Reduce
import multiprocessing
import time
import sys
import threading
from tkinter import *


class MapReduce():
	def __init__(self, root):
		self.root = root
		self.root.geometry("500x400")

		self.readDocument = ReadDocument("test.txt", 3)
		self.mapping = Mapping()
		self.shuffle = Shuffle()
		self.reduce = Reduce()

		self.text_state = "Ningun Proceso Ejecutansose"

		#Distribucion de documentos
		l1 = Label(self.root, text = "Distribucion de Archivos")
		l1.grid(row = 0, column = 0, sticky = W, pady = 2)
		self.document_name = Entry(self.root)
		self.document_name.insert(END, "test.txt")
		self.document_name.grid(row = 1, column = 0, sticky = W, pady = 2)
		self.lines_number = Entry(self.root)
		self.lines_number.insert(END, "3")
		self.lines_number.grid(row = 2, column = 0, sticky = W, pady = 2)
		button_task1 = Button(self.root, text = "Start", command = self.startThread1)
		button_task1.grid(row = 3, column = 0, sticky = W, pady = 2)
		button_task1_bug = Button(self.root, text = "Bug", command = lambda: self.readDocument.setBug(True))
		button_task1_bug.grid(row = 4, column = 0, sticky = W, pady = 2)


		#Map
		l2 = Label(self.root, text = "Map")
		l2.grid(row = 0, column = 1, sticky = W, pady = 2)
		self.n_maps = Entry(self.root)
		self.n_maps.insert(END, 6)
		self.n_maps.grid(row = 1, column = 1, sticky = W, pady = 2)
		button_task2 = Button(self.root, text = "Start", command = self.startThread2)
		button_task2.grid(row = 3, column = 1, sticky = W, pady = 2)
		button_task2_bug = Button(self.root, text = "Bug", command = lambda: self.mapping.setBug(True))
		button_task2_bug.grid(row = 4, column = 1, sticky = W, pady = 2)

		#Shuffle
		l3 = Label(self.root, text = "Shuffle")
		l3.grid(row = 0, column = 2, sticky = W, pady = 2)
		button_task3 = Button(self.root, text = "Start", command = self.startThread3)
		button_task3.grid(row = 3, column = 2, sticky = W, pady = 2)
		button_task3_bug = Button(self.root, text = "Bug", command = lambda: self.shuffle.setBug(True))
		button_task3_bug.grid(row = 4, column = 2, sticky = W, pady = 2)

		#Reduce
		l4 = Label(self.root, text = "Reduce")
		l4.grid(row = 0, column = 3, sticky = W, pady = 2)
		button_task4 = Button(self.root, text = "Start", command = self.startThread4)
		button_task4.grid(row = 3, column = 3, sticky = W, pady = 2)
		button_task4_bug = Button(self.root, text = "Bug", command = lambda: self.reduce.setBug(True))
		button_task4_bug.grid(row = 4, column = 3, sticky = W, pady = 2)

		#Estado

		self.l5 = Label(self.root, text = self.text_state, fg = "red")
		self.l5.grid(row = 7, column = 1, pady = 20)
		self.l6 = Label(self.root, text = "", fg = "purple")
		self.l6.grid(row = 8, column = 1, pady = 5)

	def startThread1(self):
		threading.Thread(target=self.taskDistribute).start()

	def startThread2(self):
		threading.Thread(target=self.taskMapping).start()

	def startThread3(self):
		threading.Thread(target=self.taskShuffle).start()

	def startThread4(self):
		threading.Thread(target=self.taskReduce).start()

	def taskDistribute(self):
		self.l5.config(text = "Task 1 en progreso", fg = "red")
		start = time.time()
		self.readDocument = ReadDocument(str(self.document_name.get()), int(self.lines_number.get()))
		self.readDocument.start()
		self.l5.config(text = "Task 1 Finalizada", fg = "green")
		self.l6.config(text = f"Tarea finalizada en: \n {time.time() - start} \nsegundos")

	def taskMapping(self):
		self.l5.config(text = "Task 2 en progreso", fg = "red")
		start = time.time()
		
		self.mapping.setNFiles(self.readDocument.getNPart())
		self.mapping.setNMaps(int(self.n_maps.get()))
		self.mapping.start()
		self.l5.config(text = "Task 2 Finalizada", fg = "green")
		self.l6.config(text = f"Tarea finalizada en: \n {time.time() - start} \nsegundos")

	def taskShuffle(self):
		self.l5.config(text = "Task 3 en progreso", fg = "red")
		start = time.time()
		self.shuffle.start()
		self.l5.config(text = "Task 3 Finalizada", fg = "green")
		self.l6.config(text = f"Tarea finalizada en: \n {time.time() - start} \nsegundos")

	def taskReduce(self):
		self.l5.config(text = "Task 4 en progreso", fg = "red")
		start = time.time()
		self.reduce.start()
		self.l5.config(text = "Task 4 Finalizada", fg = "green")
		self.l6.config(text = f"Tarea finalizada en: \n {time.time() - start} \nsegundos")

		self.reduce.fileSortedKey()



if __name__ == '__main__':
	root = Tk()
	MapReduce(root)
	root.mainloop()




