"""MicroPython MQTT Client

Implemented in accordance to 'MQTT Version 3.1.1 Plus Errata 01':
http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html

Roughly adheres to or has an API inspired by Eclipse Paho:
https://www.eclipse.org/paho/clients/python/docs/

Example usage:

    ```
    client = Client()
    client.connect('127.0.0.1')
    client.subscribe('my/cool/topic')
    client.publish('my/other/topic', "UTF-8 supported string message")
    while client.connected:
        ping_response = client.loop_read()
        if not ping_response:
            client.ping()
    ```

TODO:
  * QOS 2 support
  * Callbacks for packets other than PUBLISH
  * Retry PUBLISH packets for QOS > 0 packets

"""
import ucollections
import uselect
import usocket
from micropython import const

P_CONNECT = const(1)
P_CONNACK = const(2)
P_PUBLISH = const(3)
P_PUBACK = const(4)
P_PUBREC = const(5)
P_PUBREL= const(6)
P_PUBCOMP = const(7)
P_SUBSCRIBE = const(8)
P_SUBACK = const(9)
P_UNSUBSCRIBE = const(10)
P_UNSUBACK = const(11)
P_PINGREQ = const(12)
P_PINGRESP = const(13)
P_DISCONNECT = const(14)

Message = ucollections.namedtuple('Message', ('topic',
                                              'payload',
                                              'qos',
                                              'retain'))

class Client:
    def __init__(self, client_id='', clean_session=True, userdata=None):
        self.client_id = client_id
        self.clean_session = clean_session
        self.userdata = userdata
        self.sock = None
        self.poll = uselect.poll()
        self._size_buf = bytearray(4)
        self._packet_id = 1
        self.connected = False
        # Default on_message() callback is a printer
        self.on_message = \
            lambda client, userdata, msg: print(msg.topic, msg.payload)
        self._pub_queue = []

    def remaining_len(self, size):
        # Max size 256mb minus 1 byte
        assert size <= (256 << 20) - 1
        i = 0
        while True:
            _byte = size & 127 # mod(128)
            size >>= 7 # div(128)
            self._size_buf[i] = _byte
            if size > 0:
                self._size_buf[i] |= 128
                i += 1
                continue
            break
        return memoryview(self._size_buf)[:i+1]

    def _recv_len(self):
        value = 0
        for i in range(4):
            _byte, = self.sock.recv(1)
            value |= (_byte & 127) << 7*i
            if not _byte & 128:
                break
        else:
            raise Exception("OH NO")
        return value

    def _send(self, ctrl_typ, flags=0, payload=b''):
        if ctrl_typ in (P_PUBREL, P_SUBSCRIBE, P_UNSUBSCRIBE):
            flags = 2
        ctrl_pkt = bytes((ctrl_typ << 4 | flags, ))
        self.sock.sendall(ctrl_pkt + self.remaining_len(len(payload)))
        self.sock.sendall(payload)

    def subscribe(self, *topics, qos=0):
        # packet identifier loops around from 65535 to 1
        self._packet_id = self._packet_id + 1 & 0xffff or 1
        remaining = self._packet_id.to_bytes(2, 'big')
        for topic in topics:
            topic = topic.encode('utf8')
            remaining += len(topic).to_bytes(2, 'big')
            remaining += topic + bytes((qos,))
        self._send(P_SUBSCRIBE, payload=remaining)
        return self._packet_id

    def publish(self, topic, message=None, qos=0, retain=False):
        """Publish a MQTT message.

        returns 0 if qos is 0
        returns packet identifier if qos > 0

        In the form: [fixed_header | topic_len | topic | id* | msg]
          - id is only present if qos > 0

        """
        # Flags bitfield [qos|retain] = [00|0]
        flags = qos << 1 | retain
        topic = topic.encode('utf8')
        payload = len(topic).to_bytes(2, 'big')
        payload += topic
        if qos:
            # packet identifier loops around from 65535 to 1
            self._packet_id = self._packet_id + 1 & 0xffff or 1
            payload += self._packet_id.to_bytes(2, 'big')
        payload += message.encode('utf8')
        self._send(P_PUBLISH, flags=flags, payload=payload)
        if qos:
            self._pub_queue.append((self._packet_id, flags, payload))
            return self._packet_id

    def republish(self, flags, payload):
        # Flags bitfield [dup|qos|retain] = [0|00|0]
        flags |= 1 << 3
        self._send(P_PUBLISH, flags=flags, payload=payload)

    def ping(self):
        """Send a PINGREQ packet."""
        ctrl_pkt = P_PINGREQ << 4
        self.sock.sendall(ctrl_pkt.to_bytes(2, 'little'))

    def connect(self, host, port=1883, keepalive=60):
        """Connect to server. Returns True if session exists on server."""
        self.host = host
        self.port = port
        self.keepalive = keepalive
        return self.reconnect()

    def reconnect(self):
        """Reonnect to server. Returns True if session exists on server."""
        if self.sock:
            self.poll.unregister(self.sock)
            self.sock.close()
        self.sock = usocket.socket()
        self.sock.settimeout(self.keepalive)
        self.poll.register(self.sock, uselect.POLLIN)
        self.sock.connect(usocket.getaddrinfo(self.host, self.port)[0][-1])
        remaining = bytearray(b'\x00\x04MQTT\x04')
        remaining.append(self.clean_session << 1)
        remaining += self.keepalive.to_bytes(2, 'big')
        client_id = self.client_id.encode('utf8')
        remaining += len(client_id).to_bytes(2, 'big')
        remaining += client_id
        self._send(P_CONNECT, payload=remaining)
        ack, = self.sock.recv(1)
        assert ack >> 4 == P_CONNACK
        self.connected = True
        _, flags, ret_code = self.sock.recv(3)
        assert ret_code == 0
        return flags & 0x01

    @staticmethod
    def _unpack_publish(data, retain, qos):
        """Unpack a PUBLISH packet."""
        _data = memoryview(data)
        topic_len = int.from_bytes(_data[:2], 'big')
        _data = _data[2:]
        topic = str(_data[:topic_len], 'utf8')
        _data = _data[topic_len:]
        pid = 0
        if qos:
            pid = data[:2]
            _data = _data[2:]
        payload = str(_data, 'utf8')
        msg = Message(topic, payload, qos, retain)
        return (pid, msg)

    def loop_read(self):
        """Wait for (and process) data for half of keepalive time.

        return True if a PINGRESP packet was seen.

        Will call callback functions for appropriate packets.

        """
        timeout = self.keepalive // 2
        ping_resp = False
        while self.poll.poll(timeout * 1000):
            timeout = 0
            try:
                ctrl_pkt, = self.sock.recv(1)
            except ValueError:
                self.connected = False
                break
            ctrl_pkt_typ = ctrl_pkt >> 4
            data_len = self._recv_len()
            if data_len:
                data = self.sock.recv(data_len)
            # Start of handlers
            if ctrl_pkt_typ == P_PINGRESP:
                ping_resp = True
                continue
            if ctrl_pkt_typ == P_SUBACK:
                pid = int.from_bytes(data[:2], 'big')
                ret_code = data[2]
                # TODO: callback for on_subscribe
                continue
            if ctrl_pkt_typ == P_PUBLISH:
                retain = ctrl_pkt & 1
                qos = ctrl_pkt >> 1 & 3
                dup = ctrl_pkt >> 3 & 1
                pid, msg = self._unpack_publish(data, retain, qos)
                if qos == 1:
                    self._send(P_PUBACK, payload=pid)
                self.on_message(self, self.userdata, msg)
                continue
            if ctrl_pkt_typ == P_PUBACK:
                packet_id = int.from_bytes(data, 'big')
                for i, (_packet_id, _, _) in enumerate(self._pub_queue):
                    if packet_id == _packet_id:
                        del self._pub_queue[i]
                        break
                else:
                    print("WTF puback for unknown packet_id:", packet_id)
                continue
            if ctrl_pkt_typ == P_DISCONNECT:
                self.connected = False
                break
            print('Unhandled')
            print(ctrl_pkt)
            print(ctrl_pkt_typ)
        return ping_resp
