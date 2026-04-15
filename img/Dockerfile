FROM python:3.11-slim

WORKDIR /opt/spcs
COPY ./main.sh /opt/spcs/
COPY ./requirements.txt /opt/spcs/cfg/

RUN pip install --no-cache-dir --upgrade -r ./cfg/requirements.txt

RUN chmod 744 /opt/spcs/main.sh
ENTRYPOINT [ "/opt/spcs/main.sh" ]
