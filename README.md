
compile:
cd src
make clean
make
 
run master:
java -cp .:guava-14.0.1.jar ProcessManager

read the master host ip . ex.128.2.13.141
run slave:
java -cp .:guava-14.0.1.jar ProcessManager -c 128.2.13.141:15619

run thread:
from master's  prompt, write TestThread
>>> TestThread
