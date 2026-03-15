FROM python:3.13-slim
WORKDIR /usr/src/app
ENV PYTHONPATH=/usr/src/app

COPY ./reports ./reports
COPY ./requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

RUN useradd -M -s /bin/bash reports && chown -R reports:reports /usr/src/app
USER reports
ENV HOME=/usr/src/app

CMD [ "marimo", "edit", "--host", "0.0.0.0" ]
