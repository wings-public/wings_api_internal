FROM node:12.22.1

RUN apt-get update &&     apt-get install -y --no-install-recommends     python     wget     curl     bc     unzip     bzip2     less     bedtools     samtools     openjdk-8-jdk     tabix     vim 

ENV HOME=/root
WORKDIR /root
CMD ["bash"]
ENV JAVA_LIBRARY_PATH=/usr/lib/jni
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
RUN java -version
WORKDIR /downloads
ENV DOWNLOAD_DIR=/downloads

ENV GATK_URL=https://github.com/broadinstitute/gatk/releases/download/4.2.0.0/gatk-4.2.0.0.zip

RUN mkdir -p $DOWNLOAD_DIR &&  wget -nv -O $DOWNLOAD_DIR/gatk-4.2.0.0.zip $GATK_URL && cd $DOWNLOAD_DIR && unzip  gatk-4.2.0.0.zip

WORKDIR /wingsapi

COPY . .

RUN npm install
