Install ysql-bench on centos 7 additional required packages:
yum install -y cmake gcc-c++ freetype-devel expat-devel open-sans-fonts fontconfig

Cargo.toml:
[dependencies]
cmake = "=0.1.45"

to perform copy from on the server:
grant pg_read_server_files to yugabyte;