sudo apt update

# TODO: Check if autoremove is still needed or not
sudo apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev \
                        libreadline-dev libsqlite3-dev wget curl llvm \
                        libncurses5-dev libncursesw5-dev xz-utils tk-dev \
                        libpcap-dev libncurses-dev autoconf automake libtool pkg-config \
                        libffi-dev liblzma-dev python3-openssl git

#Get Python3.8 directly
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt install python3.8 python3.8-venv -y
echo "alias python=python3.8" >> .bashrc
source .bashrc

# Compiling Python from source. Still buggy!!!
#mkdir ~/python38
#cd ~/python38
#wget https://www.python.org/ftp/python/3.8.16/Python-3.8.16.tgz
#tar -xf Python-3.8.16.tgz
#cd Python-3.8.16

#./configure --enable-optimizations
#make -j$(nproc)
#sudo make install

#cd ../..
#echo "alias python=python3.8" >> .bashrc
#source .bashrc

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh && 
    sh get-docker.sh && 
    sudo usermod -aG docker ubuntu
sudo systemctl start docker

# Create Python env
python3.8 -m venv build_detock
source build_detock/bin/activate

sudo apt install net-tools
sudo apt install dstat -y
sudo apt install cmake build-essential pkg-config -y

# Increase TCP buffer for receiving messages at a higher throughput and avoid TCP bottlenecks
echo "net.core.rmem_max=10485760" | sudo tee -a /etc/sysctl.conf
echo "net.core.wmem_max=10485760" | sudo tee -a /etc/sysctl.conf
sudo sysctl -p  # reloads the sysctl settings

# Downgrade GCC compiler to 11 (incase we want to recopile Detock on the AWS VM)
sudo apt install g++-11 -y
export CXX=g++-11
export CC=gcc-11

# If you want to use the default iftop
sudo apt install iftop

# Compile custom iftop
#cd iftop
#chmod +x bootstrap 
#./bootstrap
#chmod +x configure 
#./configure
#make
#sudo make install
#cd ..

# Prepare directory for Docker to mount when launching containers on a remote machine
mkdir data

# Setups specific for Detock
cd Detock
#git checkout f5ae7c2ef4960aae097c32529fa7f22be39bd2c1 # Last commit before problematic PR
git checkout before-bsc-merges

# Install libraries
python3.8 -m pip install --upgrade pip
pip install -r tools/requirements.txt