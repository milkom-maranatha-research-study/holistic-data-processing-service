FROM python:3.7

ENV PYTHONUNBUFFERED 1

# Set working directory
WORKDIR /data-processing-service

# Add source code to the working directory
ADD . /data-processing-service

# Install all requirements 
RUN pip install -r requirements.txt

# Set Python PATH
ENV PYTHONPATH "${PYTHONPATH}:/holistic-data-sync/sync/"

ADD run.sh /run.sh
RUN chmod a+x /run.sh
