import jpype

classpath = "matsuSequenceFileInterface.jar"
jvmpath = "/usr/lib/jvm/java-6-sun/jre/lib/amd64/server/libjvm.so"

jpype.startJVM(jvmpath, "-Djava.class.path=%s" % classpath)
SequenceFileInterface = jpype.JClass("org.occ.matsu.SequenceFileInterface")

# SequenceFileInterface.openForWriting("test.sequencefile")
# for x in xrange(100):
#     SequenceFileInterface.write(str(x), str(x))
# SequenceFileInterface.closeWriting()

# jpype.shutdownJVM()

SequenceFileInterface.openForReading("test.sequencefile", ["22", "15", "55", "6", "99", "75", "0"])

while True:
    result = SequenceFileInterface.readNext()
    if result is None: break
    key, value = result
    print key, value
