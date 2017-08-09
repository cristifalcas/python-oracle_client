#!/usr/bin/env python
import os, sys, inspect, platform, subprocess, socket, re, time, tempfile, signal, pprint
#yum install libaio
# "/etc/profile.d/oracledb.sh"

### check if we have a local oracle client
try:
    for line in open("/etc/profile.d/oracledb.sh"):
        line = line.strip()
        if re.match( r'(^#)|([^a-z_\s])', line, re.IGNORECASE):
            continue
        values = line.split("=")
        if len(values) == 2 and not os.environ.get(values[0]):
            os.environ[values[0]] = values[1]
except IOError:
    run_remote = 1

if 'ORACLE_SID' in os.environ:
    run_remote = 0
if not run_remote:
    if not os.environ.get('ORACLE_HOME'):
        print "needs oracle_home"
        sys.exit(2)
    oracle_libs = os.environ['ORACLE_HOME']+"/lib/"
    oracle_sid = os.environ['ORACLE_SID']
    os.environ['PATH'] += ":"+os.environ['ORACLE_HOME']+"/bin/"
else:
    oracle_libs = '/root/check_db/instantclient_11_2/'
    oracle_sid = 'optymyze'

## load oracle libs
rerun = 1
if not os.environ.get('LD_LIBRARY_PATH'):
    os.environ['LD_LIBRARY_PATH'] = ":"+oracle_libs
elif not oracle_libs in os.environ.get('LD_LIBRARY_PATH'):
    os.environ['LD_LIBRARY_PATH'] += ":"+oracle_libs
else:
    rerun = 0

if rerun:
    #Reexecute ourselfs for oracle ld_path
    os.execve(os.path.realpath(__file__), sys.argv, os.environ)

discovery_list = []
hostname = socket.gethostname()
crt_dir = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0]))
# cx_oracle_libs = crt_dir+"/python"+str(sys.version_info[0])+"."+str(sys.version_info[1])+"/site-packages/"
cx_oracle_libs = crt_dir+"/python"+str(sys.version_info[0])+"."+str(sys.version_info[1])
sys.path.insert(0, cx_oracle_libs)
projects_file = crt_dir+"/"+hostname
#v1-at-db-01.synygy.net oracle_checks.discovery["oracle DBBlockBuffer_CacheHitRatio"] 99.9780474444


def make_discovery(string, name):
    #discovery_list.append("\t\t{ \"{#DB_CHECK_NAME}\":\""+string+"\" }")
    discovery_list.append("\t\t{\n\t\t\t\""+name+"\":\""+string+"\",\n\t\t\t\"{#DB_CHECK_ERR_STR}\":\""+string+" error_string\"\n\t\t}")

def make_values(string, value, error_str):
    if isinstance( value, ( int, long, float ) ):
        value = "%.2f" % value
    #string = re.sub(r'[^0-9a-zA-Z_\-\.]', r'_', string)
    value = re.sub(r'\n', r'\\n', value)
    error_str = re.sub(r'\n', r'\\\\n##############', error_str)
    discovery_list.append(hostname+" \"oracle_checks.discovery["+string+"]\" "+value)
    if error_str != "":
        print "send for "+string+" value "+value+" error string "+error_str
        discovery_list.append(hostname+" \"oracle_checks.discovery["+string+" error_string]\" "+error_str)

def run_command(cmd):
    retcode = 666
    try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        out, err = p.communicate()
        retcode = p.wait()
        if retcode < 0:
            out = "Child was terminated by signal", -retcode
    except OSError, e:
        out = "Execution failed for cmd \""+" ".join(cmd)+"\": %s" % e
    return out, retcode

def check_commands():
    ## bla bla bla, oracle intercepts SIGCHLD, opoen not worky after loading oracle module (for SYSDBA only it seems):
    #http://comments.gmane.org/gmane.comp.python.db.cx-oracle/1458
    #http://stackoverflow.com/questions/1008858/popen-communicate-throws-oserror-errno-10-no-child-processes
    out = ""
    (res, ret) = run_command(["lsnrctl", "status"])
    my_match = "\nService \""+oracle_sid+"\" has 1 instance(s).\n"
    if ret or not my_match in res:
        out += "\nlsnrctl t'sNOTok:\n we searched for: " + my_match + "\nAnd we got: \n"+"".join(res)

    (res, ret) = run_command(["tnsping", hostname])
    if ret:
        out += "\ntnsping failed with status "+str(ret)

    return out

def check_instance_up(c):
    checks_oracle_status = {
      'check1'                    : "select count(*) from v$database where upper(name)=upper('"+oracle_sid+"')",
      'check2'                    : "select count(*) from v$instance where upper(instance_name)=upper('"+oracle_sid+"')",
      }

    for check in checks_oracle_status:
        c.execute(checks_oracle_status[check])
        row = c.fetchone()
        if row[0] != 1:
            return "Got strange return value for check "+check+": "+str(row[0])
    return ""


def generic_checks(c):
    checks_generic = {
      'Connected users'                   : "select count(*) from v$session where username is not null and status='ACTIVE' and type='USER'",
      'Opened Sessions'                   : "select count(*) from v$session b where TYPE!='BACKGROUND'",
      'Percent used sessions'             : "select (select count(*) from v$session b where TYPE!='BACKGROUND') * 100 / a.value from v$parameter a where a.name = 'sessions'",
      'Uptime'                            : "select to_char((sysdate-startup_time)*86400, 'FM99999999999999990') retvalue from v$instance",
      'Users locked'                      : "select count(*) from dba_users where ACCOUNT_STATUS like 'EXPIRED(GRACE)' or ACCOUNT_STATUS like 'LOCKED(TIMED)'",
      'Number of processes'               : "select count(*) from v$process"
      }

    for check in checks_generic:
        #print "checking "+check
        key_name = check
        if (c == "discovery"):
            make_discovery (key_name, "{#DB_GLOBAL_CHECKS}")
        else:
            c.execute(checks_generic[check])
            row = c.fetchone()
            make_values (key_name, row[0], "")


def schema_check_spm(c, schema_name):
    schema_name = schema_name.upper()
    return_code = ""
    c.execute("select * from dba_directories where upper(directory_name) in ('"+schema_name+"_PDIR', '"+schema_name+"_TDIR', '"+schema_name+"_EDIR')")
    rows = c.fetchall()
    if len(rows) != 3:
        ds = [{'hello': 'there'}]
        return_code += "got wrong nr ("+str(len(rows))+" instead of 3) of special dba_directories for spm schema "+schema_name+"\n"+pprint.pformat(rows)

    return return_code


def schema_checks(c, schema_name):
    ## all must be correct+
    nr_errors = 0
    schema_name = schema_name.upper()
    return_code = ""

    #print "check 1"+str(datetime.now()-startTime))
    c.execute("select count(*) from dba_users where upper(account_status) = 'OPEN' and upper(username) ='"+schema_name+"'")
    row = c.fetchone()
    if row[0] != 1:
        return_code += "got wrong nr of dba_users for schema "+schema_name+" (there can be only one): "+str(row[0])+"\n"
        nr_errors+=1

    #print "check 2"+str(datetime.now()-startTime))
    c.execute("select count(*) from user_tablespaces where upper(tablespace_name) in ('"+schema_name+"_MTD', '"+schema_name+"_DTS', '"+schema_name+"_IND', '"+schema_name+"_TMP')")
    row = c.fetchone()
    if row[0] != 4:
        return_code += "got wrong nr of user_tablespaces for schema "+schema_name+"\n"
        nr_errors+=1

    #print "check 3"+str(datetime.now()-startTime))
    c.execute("select count(*) from dba_objects where upper(owner) = '"+schema_name+"'")
    row = c.fetchone()
    if row[0] == 0:
        return_code += "got wrong nr of dba_objects for schema "+schema_name+": "+str(row[0])+"\n"
        nr_errors+=1

    #print "check 4"+str(datetime.now()-startTime))
    c.execute("select count(*) from dba_directories where upper(directory_name) like '"+schema_name+"%'")
    row = c.fetchone()
    if row[0] == 0:
        return_code += "got wrong nr of dba_directories for schema "+schema_name+": 0 dirs\n"
        nr_errors+=1

    return (return_code,nr_errors)


def all_tablespace_usage(c):
    table_space_usage = {}
    c.execute("select tablespace_name, used_percent from dba_tablespace_usage_metrics")
    for row in c:
        table_space_usage[row[0]] = row[1]
    return table_space_usage


def get_projects_from_file():
    projects_lines = []
    try:
        for line in open(projects_file):
            line.strip()
            if re.match( r'(^#)|([^a-z_\s])', line, re.IGNORECASE):
                continue
            projects_lines.append(line)
    except IOError:
        return []
    return projects_lines


def check_schema_status(c, schema_name, key_name, first):
    status = ""
    if not first :
        status += schema_check_spm(c, schema_name)
        first = 1
    if (c == "discovery"):
        make_discovery (key_name, "{#DB_SCHEMA_STATUS}")
    else:
        (str_err, nr_err) = schema_checks(c, schema_name)
        if status != "" and nr_err == 4:
            print "everything failed. We presume the project doesn't exist anymore"
            status = ""
            str_err = ""
        status += str_err
        value = (status == "")+0
        make_values(key_name, value, status)
    return first


def check_tablespace_usage(c, table_space_name, key_name, table_space_usage):
    if (c == "discovery"):
        make_discovery (key_name, "{#DB_SCHEMA_TBLSPACE}")
    else:
        status = "NOT found tablespace usage for schema "+table_space_name
        value = -1
        if table_space_name in table_space_usage:
            status = ""
            value = table_space_usage[table_space_name]
        make_values(key_name, value, status)


def project_checks(c):
    table_space_usage = {}
    projects_lines = get_projects_from_file()
    #if (c != "discovery"):
        #table_space_usage = all_tablespace_usage(c)

    for line in projects_lines:
        prj_list = line.split()
        if len(prj_list) != 4:
            continue
        prj_name = prj_list[0]
        first = c == "discovery"

        for schema_name in [prj_list[1], prj_list[2], prj_list[3]]:
            key_name = prj_name+" "+schema_name+" status";
            first = check_schema_status(c, schema_name, key_name, first)

            #for tblsp_prefix in ['_MTD', '_DTS', '_IND', '_TMP']:
                #table_space_name = schema_name+tblsp_prefix
                #key_name = prj_name+" "+table_space_name+" tablespace usage"
                #check_tablespace_usage(c, table_space_name, key_name, table_space_usage)


def fork_my_shit():
    #http://code.activestate.com/recipes/278731-creating-a-daemon-the-python-way/
    #REDIRECT_TO = os.devnull
    REDIRECT_TO = "/tmp/oracle_checks.log"
    UMASK = 0
    WORKDIR = "/"
    MAXFD = 1024

    try:
        pid = os.fork()
    except OSError, e:
        raise Exception, "%s [%d]" % (e.strerror, e.errno)

    if (pid == 0):        # The first child.
        os.setsid()
        try:
            pid = os.fork()   # Fork a second child.
        except OSError, e:
            raise Exception, "%s [%d]" % (e.strerror, e.errno)

        if (pid == 0):      # The second child.
            os.chdir(WORKDIR)
            os.umask(UMASK)
        else:
            os._exit(0)       # Exit parent (the first child) of the second child.
    else:
        os._exit(0) # Exit parent of the first child.

    import resource
    maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if (maxfd == resource.RLIM_INFINITY):
        maxfd = MAXFD
    for fd in range(0, maxfd):
        try:
            os.close(fd)
        except OSError:
            pass
    os.open(REDIRECT_TO, os.O_RDWR|os.O_CREAT|os.O_APPEND)        # standard input (0)

    os.dup2(0, 1)                 # standard output (1)
    os.dup2(0, 2)                 # standard error (2)

    return(0)

def connectOracle(forktime):
    check_status1 = ""
    try:
        if run_remote:
            ### if we decide to tun remotelly, we need this changed
            conn = cx_Oracle.connect('system[user]/adept[pass]@v1-at-db-01.synygy.net:1521/optymyze')
        else :
            ### works only before connecting to oracle
            if forktime == "prefork":
                check_status1 = check_commands()
            conn = cx_Oracle.connect(mode = cx_Oracle.SYSDBA)

        c = conn.cursor()
        check_status2 = check_instance_up(c)
        if check_status1+check_status2 != "":
            print check_status1+check_status2
            sys.exit(1)
        #print 1
    except cx_Oracle.DatabaseError, e:
        error, = e.args
        if error.code == 1017:
            print 'Please check your credentials.'
        else:
            print "Database connection error (%s): %s" % (error.code, e)
        sys.exit(1)

    return (c, conn)


is_discovery = len(sys.argv) == 1

if is_discovery:
    c = "discovery"
else:
    import cx_Oracle
    connectOracle("prefork")
    print "1"
    sys.stdout.flush()
    fork_my_shit()
    print "====================================================="
    print time.time()
    pprint.pprint (sys.argv)
    (c, conn) = connectOracle("afterfork")

generic_checks(c)
project_checks(c)

if is_discovery:
    print "{\n\t\"data\":[\n"+",\n".join(discovery_list)+"\n\t]\n}"
else:
    c.close()
    conn.close()
    ## because fuck you oracle
    signal.signal(signal.SIGCHLD , signal.SIG_DFL)
    (fd, tmp_filename) = tempfile.mkstemp()
    tfile = os.fdopen(fd, "w")
    tfile.write("\n".join(discovery_list))
    tfile.close()
    subprocess.Popen(["zabbix_sender", "-i", tmp_filename, "-c", "/etc/zabbix_agentd.conf", "-vv"]).wait()
    os.remove(tmp_filename)
