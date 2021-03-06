import jpype

classpath = "../matsuSequenceFileInterface.jar"
jvmpath = "/usr/lib/jvm/java-6-sun/jre/lib/amd64/server/libjvm.so"

jpype.startJVM(jvmpath, "-Djava.class.path=%s" % classpath)
SequenceFileInterface = jpype.JClass("org.occ.matsu.SequenceFileInterface")

SequenceFileInterface.openForWriting("test.sequencefile", True)
for x in xrange(256):
    SequenceFileInterface.write(str(x), chr(x) + chr(x) + chr(x) + chr(x) + chr(x))
SequenceFileInterface.closeWriting()

jpype.shutdownJVM()

# SequenceFileInterface.openForReading("test.sequencefile", ["22", "15", "55", "6", "99", "75", "0"])

# while True:
#     result = SequenceFileInterface.readNext()
#     if result is None: break
#     key, value = result
#     print key, [ord(x) for x in value], ">%s<" % value
