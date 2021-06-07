# spark_streaming

# System:
* Компонента Kafka Producer відсилає запити до http://stream.meetup.com/2/rsvps і отримані відповіді відсилає на оброку до SparkStreaming.
* SparkStreaming разом з вбудованим consumer приймає ці дані та обробляє потоками. Ці дані відправляє Producer до 3-х топіків, відповідно до кожного завдання.
* В системі є 3 consumer, які ці дані приймають та зберігають у вигляді json файлу в Google Cloud Bucket. 
![Screenshot from 2021-06-07 22-01-49](https://user-images.githubusercontent.com/47101236/121074058-01b05d00-c7dc-11eb-9495-5a96ad057900.png)
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

task1.json, task2.json, task3.json - приклади результатів за 10 хв виконання
