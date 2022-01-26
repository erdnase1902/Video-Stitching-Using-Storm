FROM erdnase/vm:generic
RUN DEBIAN_FRONTEND="noninteractive" apt-get install -y openjdk-8-jdk python-is-python3 maven && apt-get clean
RUN echo "JAVA_HOME=$(jrunscript -e 'java.lang.System.out.println(java.lang.System.getProperty("java.home"));')" >> /etc/environment

# zookeeper
# https://phoenixnap.com/kb/install-apache-zookeeper
RUN mkdir -p /data/zookeeper && chown -R vagrant /data/zookeeper
RUN cd /opt && aria2c -q 'https://dlcdn.apache.org/zookeeper/zookeeper-3.7.0/apache-zookeeper-3.7.0-bin.tar.gz' && tar xf apache-zookeeper*.tar.gz && rm -rf apache-zookeeper*.tar.gz && mv apache-zookeeper* zookeeper && chown -R vagrant zookeeper
COPY resources/zoo.cfg /opt/zookeeper/conf/
RUN chown -R vagrant /opt/zookeeper/conf/zoo.cfg

# storm
# https://www.tutorialspoint.com/apache_storm/apache_storm_installation.htm
RUN cd /opt && aria2c -q 'https://dlcdn.apache.org/storm/apache-storm-2.3.0/apache-storm-2.3.0.tar.gz' && tar xf apache-storm*.tar.gz && rm -rf apache-storm*.tar.gz && mv apache-storm* storm && cd storm && mkdir data && cd .. && chown -R vagrant storm
COPY resources/storm.yaml /opt/storm/conf/
RUN chown -R vagrant /opt/storm/conf/storm.yaml

# IntelliJ
USER vagrant
RUN cd /home/vagrant && aria2c -q "https://download.jetbrains.com/product?code=ii&latest&distribution=linux" && tar xf *.tar.gz && rm *.tar.gz

USER root
