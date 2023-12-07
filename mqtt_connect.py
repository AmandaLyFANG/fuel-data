# subscriber file
import paho.mqtt.client as mqtt
import json


# Subscribe a topic
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe("lfan0920/mqtt")


def on_message(client, userdata, msg):
    print(f"Received message on topic {msg.topic}: {msg.payload}")


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# client.connect("10.86.174.51", 1883, 60)
client.connect("127.0.0.1", 1883, 60)
# Publish a message
client.publish("lfan0920/mqtt", "Hello MQTT!")

# This loop will keep the client listening for messages
client.loop_forever()
