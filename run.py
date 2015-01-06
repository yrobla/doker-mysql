#!/usr/bin/env python

from marathon import MarathonClient
import os
import random
import string
import subprocess
import shutil
import sys
import time

VOLUME_HOME="/var/lib/mysql"
CONF_FILE="/etc/mysql/conf.d/my.cnf"
LOG="/var/log/mysql/error.log"

# Set permission of config file
os.chmod(CONF_FILE, 0644)
os.chmod('/etc/mysql/conf.d/mysqld_charset.cnf', 0644)

def start_mysql():
    subprocess.call('/usr/bin/mysqld_safe &', shell=True, stderr=subprocess.STDOUT)
    i = 0
    while i<13:
        print('Waiting for confirmation of mysql service startup, trying %s/13' % str(i))
        time.sleep(5)

        # check status
        p = subprocess.Popen(['mysql', '-u', 'root', '-e', 'status'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        result = p.stdout.read()
        if result:
            break
        i += 1

    if i == 13:
        print 'Timeout starting mysql server\n'
        sys.exit(1)

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def create_mysql_user():
    start_mysql()
    mysql_user = os.getenv('MYSQL_USER')
    mysql_pass = os.getenv('MYSQL_PASS')
    if mysql_pass == '**Random**':
        os.unsetenv('MYSQL_PASS')
        mysql_pass = None

    if not mysql_pass:
        # generate random pass
        mysql_pass = id_generator(8)

    print 'Create Mysql user %s with password %s\n' % (mysql_user, mysql_pass)
    subprocess.call(['mysql', '-uroot',
        '-e', "CREATE USER '%s'@'%%' IDENTIFIED BY '%s'" % (mysql_user, mysql_pass)])
    subprocess.call(['mysql', '-uroot', '-e',
        "GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%' WITH GRANT OPTION" % mysql_user])
    print 'Done!\n'
    print '========================================================================\n'
    print 'You can now connect to this MySQL Server using:\n'
    print ' mysql -u%s -p%s -h<host> -P<port>\n' % (mysql_user, mysql_pass)
    print 'MySQL user root has no password but only allows local connections\n'
    subprocess.call(['mysqladmin', '-uroot', 'shutdown'])

def import_sql():
    start_mysql()
    if os.getenv('STARTUP_SQL'):
        filelist = os.getenv('STARTUP_SQL').split(',')
        for file in filelist:
            print 'Importing %s\n' % file
            subprocess.call(['mysql', '-uroot', '<', '"%s"' % file])
    subprocess.call(['mysqladmin', '-uroot', 'shutdown'])

if os.getenv('REPLICATION_MASTER') == '**False**':
    del os.environ['REPLICATION_MASTER']
if os.getenv('REPLICATION_SLAVE') == '**False**':
    del os.environ['REPLICATION_SLAVE']

# check if mounted volume exists
volume_path = VOLUME_HOME+'/mysql'
if not os.path.isdir(volume_path):
    print '=> An empty or uninitialized MySQL volume is detected in %s\n' % volume_path
    print 'Installing MySql....\n'

    if not os.path.isfile('/usr/share/mysql/my-default.cnf'):
        shutil.copy('etc/mysql/my.cnf', '/usr/share/mysql/my-default.cnf')

    subprocess.call(['mysql_install_db'])
    print '=> Done!\n'
    print '=> Creating admin user ...\n'

    create_mysql_user()

    if os.getenv('STARTUP_SQL'):
        print '=> Initializing DB with %s\n' % os.getenv('STARTUP_SQL')
        import_sql()
else:
    print '=> Using an existing volume of MySQL\n'

# Set MySQL REPLICATION - MASTER
if os.getenv('REPLICATION_MASTER'):
    print '=> Configuring MySQL replication as master ...\n'
    if not os.path.isfile('/replication_configured'):
        rand = id_generator(6, chars=string.digits)
        print ('=> Writing configuration file %s with server-id=%s' %
            (CONF_FILE, rand))

        # replace patterns
        subprocess.call('sed -i "s/^#server-id.*/server-id = %s/" %s' % (rand, CONF_FILE), shell=True)
        subprocess.call('sed -i "s/^#log-bin.*/log-bin = mysql-bin/" %s' % CONF_FILE, shell=True)

        print '=> Starting MySQL ...\n'
        start_mysql()

        replication_user = os.getenv('REPLICATION_USER')
        replication_pass = os.getenv('REPLICATION_PASS')
        print '=> Creating a log user %s:%s\n' % (replication_user, replication_pass)

        subprocess.call(['mysql', '-uroot', '-e',
            "CREATE USER '%s'@'%%' IDENTIFIED BY '%s'" % (replication_user, replication_pass)])
        subprocess.call(['mysql', '-uroot', '-e',
            "GRANT REPLICATION SLAVE ON *.* TO '%s'@'%%'" % replication_user])
        print '=> Done!\n'
        subprocess.call(['mysqladmin', '-uroot', 'shutdown'])
        subprocess.call(['touch', '/replication_configured'])
    else:
        print '=> MySQL replication master already configured, skip\n'

def get_master_address():
    print 'I get master ip'
    if os.getenv('MASTER_IP'):
        print 'I have master IP: %s' % os.getenv('MASTER_IP')
        return os.getenv('MASTER_IP')
    else:
        endpoint = os.getenv('MARATHON_ENDPOINT')
        peers = []
        if endpoint:
            print 'I check endpoint %s' % endpoint
            try:
                c = MarathonClient('http://%s' % endpoint)
                tasks = c.list_tasks('yroblamysqlmaster')
                for task in tasks:
                    print 'Found task %s, %s' % (task.host, task.started_at)
                    if task.started_at and task.host != os.getenv('HOST'):
                        print 'I add peer'
                        peers.append(task.host)
            except Exception as e:
                print str(e)
                pass

        print 'Have peers %s' % str(peers)
        if len(peers)>0:
            print 'I have peer %s' % peers[0]
            return peers[0]
        else:
            return None

# Set MySQL REPLICATION - SLAVE
if os.getenv('REPLICATION_SLAVE'):
    print '=> Configuring MySQL replication as slave ...\n'

    # check master addres
    master_address = get_master_address()
    if master_address:
        if not os.path.isfile('/replication_configured'):
            rand = id_generator(6, chars=string.digits)
            print '=> Writting configuration file %s with server-id=%s\n' % (CONF_FILE, rand)
            subprocess.call('sed -i "s/^#server-id.*/server-id = %s/" %s' % (rand, CONF_FILE), shell=True)
            subprocess.call('sed -i "s/^#log-bin.*/log-bin = mysql-bin/" %s' % CONF_FILE, shell=True)
            print '=> Starting MySQL ...\n'
            start_mysql()

            print '=> Setting master connection info on slave\n'
            subprocess.call(['mysql', '-uroot', '-e',
                "CHANGE MASTER TO MASTER_HOST='%s',MASTER_USER='%s',MASTER_PASSWORD='%s',MASTER_PORT=3306, MASTER_CONNECT_RETRY=30" %
                (master_address, os.getenv('MYSQL_ENV_REPLICATION_USER'), os.getenv('MYSQL_ENV_REPLICATION_PASS'))])
            print '=> Done!\n'
            subprocess.call(['mysqladmin', '-uroot', 'shutdown'])
            subprocess.call(['touch', '/replication_configured'])
        else:
            print '=> MySQL replicaiton slave already configured, skip\n'
    else:
        print '=> Cannot configure slave, please be sure that master is spinned\n'
        sys.exit(1)

sys.stdout.flush()
subprocess.call('/usr/bin/mysqld_safe &', shell=True, stderr=subprocess.STDOUT)

while True:
    time.sleep(1)
