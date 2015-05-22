FROM junprtcl/crawler

WORKDIR /crawlTwitter
ADD . /crawlTwitter
RUN pip install -r /crawlTwitter/requirements.txt