import jpype
import time

classpath = "../matsuSequenceFileInterface.jar"
jvmpath = "/usr/lib/jvm/java-6-sun/jre/lib/amd64/server/libjvm.so"

jpype.startJVM(jvmpath, "-Djava.class.path=%s" % classpath)
SequenceFileInterface = jpype.JClass("org.occ.matsu.SequenceFileInterface")

for size in xrange(5, 9):
    SequenceFileInterface.openForReading("test_%d.sequencefile" % size)

    before = time.time()
    tmp = SequenceFileInterface.read("small")
    after = time.time()

    print "time:", (after - before)

# % python testScaling-read.py 
# SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
# SLF4J: Defaulting to no-operation (NOP) logger implementation
# SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
# time: 0.14453291893
# time: 1.1416208744
# time: 11.8242108822
# time: 124.023122072

# Nope, just reading the keys doesn't scale well when the values are large.
# It must not be jumping over the data, as I would expect it to.
