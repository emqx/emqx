from stompest.config import StompConfig
from stompest.protocol import StompSpec
from stompest.sync import Stomp

CONFIG = StompConfig('tcp://localhost:61613', version=StompSpec.VERSION_1_1)
QUEUE = '/queue/test'

if __name__ == '__main__':
  client = Stomp(CONFIG)
  client.connect(heartBeats=(0, 10000))
  client.subscribe(QUEUE, {StompSpec.ID_HEADER: 1, StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL})
  client.send(QUEUE, 'test message 1')
  client.send(QUEUE, 'test message 2')
  while True:
    frame = client.receiveFrame()
    print 'Got %s' % frame.info()
    client.ack(frame)
  client.disconnect()

