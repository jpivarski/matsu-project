import jpype

classpath = "../matsuSequenceFileInterface.jar"
# jvmpath = jpype.getDefaultJVMPath()
jvmpath = "/usr/lib/jvm/java-6-sun/jre/lib/amd64/server/libjvm.so"

jpype.startJVM(jvmpath, "-Djava.class.path=%s" % classpath)
SequenceFileInterface = jpype.JClass("org.occ.matsu.SequenceFileInterface")

# try:
#     SequenceFileInterface.openForWriting("test.sequencefile")
# except jpype.JavaException as exception:
#     print exception.message()
#     print exception.stacktrace()

# SequenceFileInterface.write("one", "111")
# SequenceFileInterface.write("two", "222")
# SequenceFileInterface.write("twotwo", "2 2")

# SequenceFileInterface.closeWriting()

# jpype.shutdownJVM()

try:
    SequenceFileInterface.openForReading("test.sequencefile")
except jpype.JavaException as exception:
    print exception.message()
    print exception.stacktrace()

print SequenceFileInterface.read("asdf")
