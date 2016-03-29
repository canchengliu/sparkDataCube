import sys
gvar = None

def f1():
	gvar = 1
	print gvar

def f2():
	global gvar
	gvar = 2
	print gvar
	f1()

if __name__ == '__main__':
	f2()
