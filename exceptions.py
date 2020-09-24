
class Error(Exception):
	def __init__(self, message="Error"):
		self.message = message

	def __str__(self):
		return repr(self.message)

	def raiseExeption(self, msg):
		self.message = msg
		print(self.message)
		raise Error

