"""
Fundamentals of IoT - Lab 10: Protocol Buffers
Student Names: Kacper Palka, Thomas Burke, Denis Bajgora, Tomas Mac Sweeney
Student Numbers: C22376553, C22737179, C22503876, C21463872
"""

import umqtt.robust as umqtt
from network import WLAN
import time
from machine import Pin, ADC, Timer
import my_schema_upb2


# ---------- Wi-Fi Connection ----------
def connect(wifi_obj, ssid, password, timeout=10):
    wifi_obj.connect(ssid, password)
    while timeout > 0:
        if wifi_obj.status() != 3:
            time.sleep(1)
            timeout -= 1
        else:
            print("Connected to Wi-Fi")
            return True
    return False


ssid = "Ya"
password = "12345678"

wifi = WLAN(WLAN.IF_STA)
wifi.active(True)

if not connect(wifi, ssid, password):
    print("Wi-Fi connection failed.")

# ---------- MQTT Configuration ----------
BROKER_IP = "172.20.10.14"
PORT = 1883
TOPIC = "temp/pico"
OUTPUT_PIN = None
PUB_IDENT = "pico1"

# Initialize LED only if OUTPUT_PIN is set
if OUTPUT_PIN is not None:
    led = Pin(OUTPUT_PIN, Pin.OUT)
else:
    led = None

# Publisher data format:
# { "pico1": {"temperature": xx.x, "epoch_time": 1234567890 } }
publisher_data = {}

# 10 minutes timeout
TIMEOUT_SECONDS = 600


# ---------- Cleanup Old Publishers ----------
def cleanup_old_publishers():
    current_epoch_time = time.time()
    to_delete = []

    # Check if publisher is older than 10 minutes
    for pub_id, data in publisher_data.items():
        # Calculate the age of the publisher
        age = current_epoch_time - data["epoch_time"]
        print(f"Checking {pub_id} age: {age:.1f}s")
        if age > TIMEOUT_SECONDS:
            to_delete.append(pub_id)

    # Delete the publisher if it is older than 10 minutes
    for pub_id in to_delete:
        del publisher_data[pub_id]
        print(f"Removed inactive publisher: {pub_id}")


# ---------- Calculate Average Temperature ----------
def calculate_average_temperature():
    # If no publishers, return 0.0
    if not publisher_data:
        return 0.0
    # Calculate the total temperature of the publishers
    total = sum(data["temperature"] for data in publisher_data.values())
    # Calculate the average temperature
    return total / len(publisher_data)


# ---------- Callback Function ----------
def callback(topic, message):
    # Decode the topic and message
    topic = topic.decode()
    message = message.decode()

    # If the topic is the same as the topic we are subscribed to
    if topic == TOPIC:
        # Parse the message into a SensorDataMessage object
        sensorData = my_schema_upb2.SensordataMessage()
        sensorData.parse(message)

        # Get the temperature, publisher ID, and epoch time from the SensorDataMessage object
        temperature = sensorData.temperature._value
        publisher_id = sensorData.publisher_id._value
        epoch_time = sensorData.current_time._value

        # Print the temperature, publisher ID, and epoch time
        print(
            f"Temperature received: {temperature:.2f}°C from {publisher_id} at {epoch_time}"
        )
        publisher_data[publisher_id] = {
            "temperature": temperature,
            "epoch_time": epoch_time,
        }

        cleanup_old_publishers()
    else:
        print(f"Ignored message on topic: {topic}")


# ---------- Display Average Temperature ----------
def display_average_temp(timer):
    # Calculate the average temperature
    avg_temp = calculate_average_temperature()
    # Print the average temperature
    print(
        f"Current Average temp from {len(publisher_data)} publishers: {avg_temp:.2f}°C"
    )
    # Turn on the LED if the average temperature is greater than 30°C
    led.value(1 if avg_temp > 30 else 0)


# ---------- Subscriber Function ----------
def subscriber():
    # Set the callback function
    mqtt.set_callback(callback)
    mqtt.connect()
    mqtt.subscribe(TOPIC.encode())
    print(f"Subscribed to topic: {TOPIC}")

    # Initialize the cleanup and display timers
    cleanup_timer = Timer()
    display_timer = Timer()

    # Initialize the cleanup timer to clean up old publishers every 10 seconds
    cleanup_timer.init(
        period=10000, mode=Timer.PERIODIC, callback=lambda t: cleanup_old_publishers()
    )

    # Initialize the display timer to display the average temperature every 5 seconds
    display_timer.init(period=5000, mode=Timer.PERIODIC, callback=display_average_temp)


# ---------- Publisher Function ----------
def publisher():
    # Initialize the temperature timer to read the temperature every 2 seconds
    temp_timer = Timer()
    temp_timer.init(period=2000, mode=Timer.PERIODIC, callback=read_temp_send_data)


# ---------- Read Temperature and Send Data ----------
def read_temp_send_data(timer):
    global sensor_temp

    # Initialize the SensorDataMessage object
    sensorData = my_schema_upb2.SensordataMessage()

    # Read the temperature from the sensor
    value = sensor_temp.read_u16()
    voltage = value * (3.3 / (2**16))
    temperature = 27 - (voltage - 0.706) / 0.001721

    # Set the temperature, publisher ID, and current time in the SensorDataMessage object
    sensorData.temperature = temperature
    sensorData.publisher_id = PUB_IDENT
    sensorData.current_time = time.time()

    # Print the current time
    print(f"Publishing timestamp: {sensorData.current_time}")

    # Publish the SensorDataMessage object to the MQTT broker
    mqtt.publish(TOPIC, sensorData.serialize())


# ---------- Main mode selection ----------
# If the output pin is not None and the publisher ID is None, run the subscriber function
if OUTPUT_PIN != None and PUB_IDENT == None:
    # Initialize the MQTT client for subscriber
    mqtt = umqtt.MQTTClient(
        b"subscriber", BROKER_IP.encode(), port=PORT, keepalive=7000
    )
    # Run the subscriber function
    subscriber()
    # Wait for messages from the MQTT broker
    while True:
        mqtt.wait_msg()

elif OUTPUT_PIN == None and PUB_IDENT != None:
    # Initialize the temperature sensor
    sensor_temp = ADC(4)
    # Initialize the MQTT client for publisher
    mqtt = umqtt.MQTTClient(b"publisher", BROKER_IP.encode(), port=PORT, keepalive=7000)
    # Connect to the MQTT broker
    mqtt.connect()
    # Run the publisher function
    publisher()
    while True:
        time.sleep(1)
else:
    print("invalid configuration")
