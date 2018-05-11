FROM anapsix/alpine-java:8_jdk
COPY requirements.txt /tmp/requirements.txt
RUN apk add --update curl git unzip python3 py-pip && pip install -U py4j  
RUN pip3 install --user -r /tmp/requirements.txt    
ADD sparksql.py /
ADD ratings.csv /
CMD ["python3", "./sparksql.py"]

