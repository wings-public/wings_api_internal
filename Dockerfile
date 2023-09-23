FROM ubuntu:focal 

# Set timezone:
RUN ln -snf /usr/share/zoneinfo/$CONTAINER_TIMEZONE /etc/localtime && echo $CONTAINER_TIMEZONE > /etc/timezone

# Install dependencies:
#RUN apt-get update && apt-get install -y tzdata

RUN apt-get -y update && apt-get install -y --no-install-recommends tzdata software-properties-common python-is-python3     wget     curl     bc     unzip     bzip2     less     bedtools     samtools         tabix     vim build-essential libkrb5-dev
#RUN add-apt-repository -y ppa:openjdk-r/ppa

RUN apt-get update && apt-get install -y openjdk-11-jdk 
ENV HOME=/root
WORKDIR /root
CMD ["bash"]
ENV JAVA_LIBRARY_PATH=/usr/lib/jni
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/
RUN java -version
WORKDIR /downloads
ENV DOWNLOAD_DIR=/downloads

ENV GATK_URL=https://github.com/broadinstitute/gatk/releases/download/4.2.0.0/gatk-4.2.0.0.zip
RUN curl -sL https://deb.nodesource.com/setup_18.x -o /tmp/nodesource_setup.sh
RUN bash /tmp/nodesource_setup.sh

RUN apt-get -y install nodejs

RUN mkdir -p $DOWNLOAD_DIR &&  wget -nv -O $DOWNLOAD_DIR/gatk-4.2.0.0.zip $GATK_URL && cd $DOWNLOAD_DIR && unzip  gatk-4.2.0.0.zip

WORKDIR /wingsapi

COPY . .

RUN npm install

# docker build -t wingsorg/wingsapiuat:v4.0.2 .
