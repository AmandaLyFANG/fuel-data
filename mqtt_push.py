# mqtt_push file, explore more possible functions here.
import paho.mqtt.publish as publish

publish.single("lfan0920/mqtt", "Hello World", hostname='localhost', port=1883)

print('finish')