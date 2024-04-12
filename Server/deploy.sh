#deploy.sh file
#---------------------------
#!/bin/bash
# mysql -h localhost -u root -pabc -e "CREATE DATABASE IF NOT EXISTS distributed_database;"
# psql -h localhost -U postgres -w abc -c "CREATE DATABASE IF NOT EXISTS distributed_database;"
python3 server.py &