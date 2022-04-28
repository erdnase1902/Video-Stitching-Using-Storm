FROM erdnase/vm:generic
RUN apt-get update; DEBIAN_FRONTEND="noninteractive" apt-get install -y ffmpeg openjdk-8-jdk python-is-python3 maven gpicview vlc imagemagick && apt-get clean
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

# Sample Project
RUN cd /home/vagrant && git clone https://github.com/ADMIcloud/examples.git && git clone https://github.com/Coveros/helloworld

# Sample Video preprocessed for convenience
USER root
RUN pip3 install youtube-dl pillow
USER vagrant
RUN cd /home/vagrant && mkdir sample-video && cd sample-video && youtube-dl -f 137  https://www.youtube.com/watch?v=z3frSkVJqLw
RUN cd /home/vagrant/sample-video/ && mkdir frames && cd frames && ffmpeg -i ../*z3frSkVJqLw.mp4 -vf fps=30 out%d.jpg
COPY resources/crop_img.py /home/vagrant
RUN cd /home/vagrant/sample-video/ && mkdir frames_split && cd /home/vagrant && python3 crop_img.py
RUN cd /home/vagrant && mkdir out_imgs && mkdir out_video
# Sample Video - By Resolution - More choices
RUN cd /home/vagrant/ && mkdir sample-video-by-resolution
RUN cd /home/vagrant/sample-video-by-resolution && mkdir 256x144 && cd 256x144 && youtube-dl --rm-cache-dir && youtube-dl -f 160  https://www.youtube.com/watch?v=z3frSkVJqLw && mkdir frames && mkdir frames_split && cd frames && ffmpeg -i ../*z3frSkVJqLw.mp4 -vf fps=30 out%d.jpg && cd /home/vagrant && python3 crop_img.py '/home/vagrant/sample-video-by-resolution/256x144/frames/' '/home/vagrant/sample-video-by-resolution/256x144/frames_split'
RUN cd /home/vagrant/sample-video-by-resolution && mkdir 426x240 && cd 426x240 && youtube-dl --rm-cache-dir && youtube-dl -f 133  https://www.youtube.com/watch?v=z3frSkVJqLw && mkdir frames && mkdir frames_split && cd frames && ffmpeg -i ../*z3frSkVJqLw.mp4 -vf fps=30 out%d.jpg && cd /home/vagrant && python3 crop_img.py '/home/vagrant/sample-video-by-resolution/426x240/frames/' '/home/vagrant/sample-video-by-resolution/426x240/frames_split'
RUN cd /home/vagrant/sample-video-by-resolution && mkdir 640x360 && cd 640x360 && youtube-dl --rm-cache-dir && youtube-dl -f 134  https://www.youtube.com/watch?v=z3frSkVJqLw && mkdir frames && mkdir frames_split && cd frames && ffmpeg -i ../*z3frSkVJqLw.mp4 -vf fps=30 out%d.jpg && cd /home/vagrant && python3 crop_img.py '/home/vagrant/sample-video-by-resolution/640x360/frames/' '/home/vagrant/sample-video-by-resolution/640x360/frames_split'
RUN cd /home/vagrant/sample-video-by-resolution && mkdir 854x480 && cd 854x480 && youtube-dl --rm-cache-dir && youtube-dl -f 135  https://www.youtube.com/watch?v=z3frSkVJqLw && mkdir frames && mkdir frames_split && cd frames && ffmpeg -i ../*z3frSkVJqLw.mp4 -vf fps=30 out%d.jpg && cd /home/vagrant && python3 crop_img.py '/home/vagrant/sample-video-by-resolution/854x480/frames/' '/home/vagrant/sample-video-by-resolution/854x480/frames_split'
RUN cd /home/vagrant/sample-video-by-resolution && mkdir 1280x720 && cd 1280x720 && youtube-dl --rm-cache-dir && youtube-dl -f 136  https://www.youtube.com/watch?v=z3frSkVJqLw && mkdir frames && mkdir frames_split && cd frames && ffmpeg -i ../*z3frSkVJqLw.mp4 -vf fps=30 out%d.jpg && cd /home/vagrant && python3 crop_img.py '/home/vagrant/sample-video-by-resolution/1280x720/frames/' '/home/vagrant/sample-video-by-resolution/1280x720/frames_split'
RUN cd /home/vagrant/sample-video-by-resolution && mkdir 1920x1080 && cd 1920x1080 && youtube-dl --rm-cache-dir && youtube-dl -f 137  https://www.youtube.com/watch?v=z3frSkVJqLw && mkdir frames && mkdir frames_split && cd frames && ffmpeg -i ../*z3frSkVJqLw.mp4 -vf fps=30 out%d.jpg && cd /home/vagrant && python3 crop_img.py '/home/vagrant/sample-video-by-resolution/1920x1080/frames/' '/home/vagrant/sample-video-by-resolution/1920x1080/frames_split'


# Create config file for input file location
RUN touch /home/vagrant/stitch.config && echo '/home/vagrant/sample-video/frames_split' >> /home/vagrant/stitch.config

USER root
