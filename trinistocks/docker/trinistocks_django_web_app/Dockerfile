# Pull base image from dockerhub
FROM python:3.10
ENV TZ="America/Port_of_Spain"
# create the directory to store our project files
RUN mkdir /trinistocks
# Set the working directory inside the container
WORKDIR /trinistocks
RUN apt update
# install some package dependencies that we need
RUN apt install unixodbc-dev unixodbc  --yes
# now set up poetry for python package management
RUN pip install 'poetry==1.1.13'
# Copy the pipfile and .lock so that we can install our required python packages
COPY ../../pyproject.toml poetry.lock /trinistocks/
# set up the python packages using poetry
RUN poetry config virtualenvs.create false \
  && poetry install $(test "$ENVIRON" == production && echo "--no-dev") --no-interaction --no-ansi
# Copy project files into container
COPY ../../.. /trinistocks/