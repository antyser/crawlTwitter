FROM junprtcl/crawler

WORKDIR /crawlTwitter
ADD . /crawlTwitter
RUN pip install -r /crawlTwitter/requirements.txt
RUN apt-get install wget
RUN apt-get install unzip
RUN wget https://github.com/Parsely/pykafka/archive/master.zip
RUN cd pykafka-master; python setup.py install
RUN cd
