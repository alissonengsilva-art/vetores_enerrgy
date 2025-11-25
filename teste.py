
import struct, snap7
c = snap7.client.Client()
c.connect("192.168.0.1", 0, 1)
data = c.db_read(1, 0, 24)
print(struct.unpack(">6f", data))
c.disconnect()
