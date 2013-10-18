1 Build
● To clean the build, under /src directory, type:
$ make clean
This will remove the prebuilt
class files.
● To build the project, under /src directory, type:
$ make

2 Run ProcessManager
● In order to run ProcessManager in Master, under /src directory, type:
$ java cp
.:guava14.0.1.
jar ProcessManager
If successfully executed, the output will show the IP address of the Master, with the
message ‘This is Master!’ and a prompt.
● In order to run ProcessManager in Slave, under /src directory, type:
$ java cp
.:guava14.0.1.
jar ProcessManager c
<ip of Master>:15619
If successfully executed, the output will show the IP address of the Slave, with the
message ‘This is Slave!’
● After the Slaves are added, in Master’s prompt, type:
>>> <name of thread>
It can start running the thread in the Slave.
In Master’s prompt, type:
>>> ps
We can check the status of all the Slaves.
In Master’s prompt, type:
>>> quit
This will terminate the Master and Slave.
