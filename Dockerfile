FROM quay.io/astronomer/astro-runtime:5.0.1
ENV POST_URL https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
RUN wget ${POST_URL}
RUN mkdir -p ~/pyspark/jars
RUN mkdir -p ~/pyspark/data
RUN mv postgresql-42.6.0.jar ~/pyspark/jars


# RUN wget https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
# RUN tar xvf spark-*
# RUN rm spark-3.3.2-bin-hadoop3.tgz
# RUN mkdir -p ~/pyspark/spark
# RUN mv spark-3.3.2-bin-hadoop3 ~/pyspark/spark