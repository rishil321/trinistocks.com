# Dockerfile for trinistocks background scripts
# Pull base image from dockerhub
FROM python:3.10
# create the directory to store our project files
RUN mkdir /trinistocks
# Set the working directory inside the container
WORKDIR /trinistocks
RUN apt update
# install some package dependencies that we need
RUN apt install cron unixodbc-dev dos2unix unixodbc --yes
# Copy finsys_cron_jobs file to the cron.d directory
COPY /scripts/trinistocks_scripts_cronjobs /etc/cron.d/trinistocks_scripts_cronjobs
# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/trinistocks_scripts_cronjobs
# Apply cron job
RUN dos2unix /etc/cron.d/trinistocks_scripts_cronjobs && crontab /etc/cron.d/trinistocks_scripts_cronjobs
# Create the log file to be able to run tail
RUN touch /var/log/cron.log && chmod 777 -R /var/log
# now set up poetry for python package management
RUN pip install 'poetry==1.1.13'
# Copy the pipfile and .lock so that we can install our required python packages
COPY pyproject.toml poetry.lock /trinistocks/
# set up the python packages using poetry
RUN poetry config virtualenvs.create false \
  && poetry install $(test "$ENVIRON" == production && echo "--no-dev") --no-interaction --no-ansi
# Copy project files into container
COPY ../../.. /trinistocks/
RUN DEBIAN_FRONTEND=noninteractive apt-get -y install postfix libsasl2-modules
COPY ../../postfix/ /etc/postfix/
