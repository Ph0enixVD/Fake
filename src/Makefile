JFLAGS = -cp
JC = javac
JAR = guava-14.0.1.jar
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) .:$(JAR) $*.java

CLASSES = \
	MigratableProcess.java \
	ProcessManager.java \
	Message.java \
	MessageType.java \
	Server.java \
	TestThread.java \

default: classes

classes: $(CLASSES:.java=.class)

clean:
	$(RM) *.class
	
