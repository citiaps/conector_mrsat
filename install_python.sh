Sudo apt update & sudo apt upgrade
sudo apt isntall git
git clone https://github.com/python/cpython.git
cd cpython
git checkout v3.9.2
sudo apt install wget build-essential libreadline-gplv2-dev libncursesw5-dev \
libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev libffi-dev zlib1g-dev libpq-dev libgdbm-compat-dev
. /configure --enable-optimizations
make -j 4
sudo make install
done
