FROM ubuntu:trusty
MAINTAINER Yolanda Robla <info@ysoft.biz>

# Install packages
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update
RUN apt-get -yq install mysql-server-5.6 pwgen

# install python and deps
RUN apt-get install -y python python-dev python-pip
RUN pip install marathon
RUN rm -rf /var/lib/apt/lists/*

# Remove pre-installed database
RUN rm -rf /var/lib/mysql/*

# Add MySQL configuration
ADD my.cnf /etc/mysql/conf.d/my.cnf
ADD mysqld_charset.cnf /etc/mysql/conf.d/mysqld_charset.cnf

# Add MySQL scripts
ADD run.py /run.py
ADD import_sql.sh /import_sql.sh
RUN chmod 755 /*.py
RUN chmod 755 /*.sh

# Exposed ENV
ENV MYSQL_USER admin
ENV MYSQL_PASS **Random**

# Replication ENV
ENV REPLICATION_MASTER **False**
ENV REPLICATION_SLAVE **False**
ENV REPLICATION_USER replica
ENV REPLICATION_PASS replica

# Add VOLUMEs to allow backup of config and databases
VOLUME ["/etc/mysql", "/var/lib/mysql"]
EXPOSE 3306
CMD ["/run.py"]
