# Dockerfile for trinistocks background scripts
# Pull base image from dockerhub
FROM python:3.10
# create the directory to store our project files
RUN mkdir /trinistocks
# Set the working directory inside the container
WORKDIR /trinistocks
# Adding trusting keys to apt for repositories
RUN wget --no-check-certificate -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
# Adding Google Chrome to the repositories
RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
# Updating apt to see and install Google Chrome
RUN DEBIAN_FRONTEND=noninteractive apt-get -y update
RUN DEBIAN_FRONTEND=noninteractive apt-get install --allow-unauthenticated -yqq postfix cron dos2unix rsyslog unzip unixodbc-dev unixodbc libsasl2-modules libglib2.0-0 libnss3 libgconf-2-4 libfontconfig1
# RUN apt-get install --allow-unauthenticated -y
# Set display port as an environment variable
ENV DISPLAY=:99
# copy the postfix config files
COPY postfix /etc/postfix/
# copy the rsyslog config files
COPY rsyslog.conf /etc/
# install the latest version of google chrome
RUN DEBIAN_FRONTEND=noninteractive apt-get install --allow-unauthenticated -yqq google-chrome-stable
# Download the Chrome Driver
RUN wget --no-check-certificate -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip
# Unzip the Chrome Driver into /usr/local/bin directory
RUN unzip /tmp/chromedriver.zip chromedriver -d /usr/local/bin/
# Copy finsys_cron_jobs file to the cron.d directory
COPY /scripts/trinistocks_scripts_cronjobs /etc/cron.d/trinistocks_scripts_cronjobs
# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/trinistocks_scripts_cronjobs
# Apply cron job
RUN dos2unix /etc/cron.d/trinistocks_scripts_cronjobs && crontab /etc/cron.d/trinistocks_scripts_cronjobs
# Create the log file to be able to run tail
RUN touch /var/log/cron.log && chmod 777 -R /var/log
# now set up poetry for python package management
RUN pip install poetry
# Copy the pipfile and .lock so that we can install our required python packages
COPY pyproject.toml poetry.lock /trinistocks/
# set up the python packages using poetry
RUN poetry config virtualenvs.create false \
  && poetry install $(test "$ENVIRON" == production && echo "--no-dev") --no-interaction --no-ansi
# Copy project files into container
COPY . /trinistocks/