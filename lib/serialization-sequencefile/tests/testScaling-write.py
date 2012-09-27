import jpype
import random

classpath = "../matsuSequenceFileInterface.jar"
jvmpath = "/usr/lib/jvm/java-6-sun/jre/lib/amd64/server/libjvm.so"

jpype.startJVM(jvmpath, "-Djava.class.path=%s" % classpath)
SequenceFileInterface = jpype.JClass("org.occ.matsu.SequenceFileInterface")

sample = "".join([chr(random.randint(48, 123)) for x in xrange(10**5)])

for size in xrange(5, 9):
    print "making values with 10**%d bytes" % size
    towrite = sample * 10**(size - 5)

    print "writing them to the file"
    SequenceFileInterface.openForWriting("test_%d.sequencefile" % size)

    for N in xrange(100):
        SequenceFileInterface.write(str(N), towrite)
        SequenceFileInterface.sync()

    SequenceFileInterface.write("small", "itty-bitty")
    SequenceFileInterface.sync()

    SequenceFileInterface.closeWriting()

jpype.shutdownJVM()

# % lsl *.sequencefile
# -rwxr-xr-x 1 pivarski pivarski 9.6M Sep 26 14:22 test_5.sequencefile*
# -rwxr-xr-x 1 pivarski pivarski  96M Sep 26 14:22 test_6.sequencefile*
# -rwxr-xr-x 1 pivarski pivarski 954M Sep 26 14:23 test_7.sequencefile*
# -rwxr-xr-x 1 pivarski pivarski 9.4G Sep 26 14:26 test_8.sequencefile*
