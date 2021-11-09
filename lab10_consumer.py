from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from matplotlib.animation import FuncAnimation
from datetime import datetime
import numpy as np
from windrose import WindroseAxes
from constants import KAFKA_TOPIC, wind_options

# Commented code is for unrestricted
fig = plt.figure()
ax = fig.add_subplot(1,1,1)
def plot(_, x, y, y2, consumer):
    message = ''
    for msg in consumer:
        message = msg
        break

    x.append(datetime.now().strftime('%H:%M:%S'))
    # message = json.loads(message.value.decode())
    message = msg.value.decode('ASCII')
    data = []
    for i in message:
        data.append(i)
    # y.append(message['temperature'])
    y.append(ord(data[0]))
    # y2.append(message['humidity'])
    y2.append(ord(data[1]))
    ax.clear()
    ax.plot(x, y)
    ax.plot(x, y2)
    plt.title('Termómetro y Higrómetro')
    plt.xlabel('T')
    plt.ylabel('V')
    plt.legend(['Temperature', 'Humidity'])
    plt.xticks(rotation=70)
    plt.subplots_adjust(bottom=0.2)

def wind_rose():
    ws = np.random.random(500) * 6
    wd = np.random.random(500) * 360

    wind_axes = WindroseAxes.from_ax()
    wind_axes.contourf(wd, ws, bins=np.arange(0, 8, 1), cmap=cm.hot)
    wind_axes.contour(wd, ws, bins=np.arange(0, 8, 1), colors='black')
    wind_axes.set_xticklabels(wind_options)
    wind_axes.set_theta_zero_location('N')
    plt.show()


kafka_consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers='20.120.14.159:9092', group_id='group_los_machos')
temperatures, humidities, wind_drections, times = [], [], [], []

# Plot through time
# wind_rose()
graph = FuncAnimation(fig, plot, fargs=(times, temperatures, humidities, kafka_consumer), interval=10)
plt.show()


for msg in kafka_consumer:

    # Decode a json
    # message_ = msg.value.decode()
    # message = json.loads(message_)

    # temperatures.append(message['temperature'])
    # humidities.append(message['humidity'])
    # wind_drections.append(message['wind direction'])

    # time = datetime.now()
    # times.append(time.strftime('%H:%M:%S'))

    message = msg.value.decode('ASCII')
    print("Message ", message)
    data = []
    for i in message:
        data.append(i)
    temp = ord(data[0])
    temperatures.append(temp)

    humidity = ord(data[1])
    humidities.append(humidity)

    wind = wind_options[ord(data[2])]
    wind_drections.append(wind)
