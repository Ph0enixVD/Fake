JFLAGS =
JC = javac
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
	MigratableProcess.java \
	ProcessManager.java \

default: classes

classes: $(CLASSES:.java=.class)

clean:
	$(RM) *.class
	
