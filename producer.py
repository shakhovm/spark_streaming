from kafka import KafkaProducer
import requests


if __name__ == "__main__":
	producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

	r = requests.get("https://stream.meetup.com/2/rsvps", stream=True)

	for line in r.iter_lines():
		producer.send('meetsup-input', line)

