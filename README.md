# spark_streaming

# System:
* Компонента Kafka Producer відсилає запити до http://stream.meetup.com/2/rsvps і отримані відповіді відсилає на оброку до SparkStreaming.
* SparkStreaming разом з вбудованим consumer приймає ці дані та обробляє потоками. Ці дані відправляє Producer до 3-х топіків, відповідно до кожного завдання.
* В системі є 3 consumer, які ці дані приймають та зберігають у вигляді json файлу в Google Cloud Bucket. 

# Application Requirement
* Для того, щоб аплікація працювала коректно, має бути версія пайтону Python 3.7.* та спарку Spark 2.4.

# Run Application
* consumer.py - відповідає за consumer, який має приймати оброблені результати. Для трьох тасків потрібно запустити трьох консюмерів. Запускати потрібно наступним чином
--------------------------

```
$ python consumer.py <config-file>


```
\<config-file\> - Приклад такого файлу є на гіті

* producer.py - Відповідає за продюсера, який посилає запити на сайт

--------------------------

```
$ python producer.py <host:port>


```
* main.py - Основа аплікація для оброблення даних за допомогою spark-streaming

--------------------------

```
$ spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.8 main.py <producer-host:port> <consumer-host:port>



```

\<producer-host:port\> - хост продюсера, який пересилає оброблений результат

\<consumer-host:port\> - хост консюмера, який отримує дані для обробки

# Examples

task1.json, task2.json, task3.json - приклади результатів
