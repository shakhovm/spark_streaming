from kafka import KafkaProducer
import requests
import sys


if __name__ == "__main__":
	producer = KafkaProducer(bootstrap_servers=[sys.argv[1]]) #host:9092

	r = requests.get("https://stream.meetup.com/2/rsvps", stream=True)

	for line in r.iter_lines():
		try:
			producer.send('meetsup-input', line)
		except Exception as er:
			print(er)


