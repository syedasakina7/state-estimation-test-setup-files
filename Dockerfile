FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Europe/Helsinki

# Install OS dependencies
RUN apt-get update && \
    apt-get install -y tzdata octave python3 python3-pip && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
    dpkg-reconfigure -f noninteractive tzdata && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip3 install paho-mqtt scipy numpy

# Set working directory
WORKDIR /app

# Copy all necessary files to the container
COPY . /app

# Create a script that runs both the MQTT bridge and the runner pro-cesses
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

# Set the entrypoint to run the script
CMD ["/bin/bash", "/app/run.sh"]
