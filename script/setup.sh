if [[ "$OSTYPE" == "linux-gnu" ]]; then
    if [ -f /etc/redhat-release ]; then
        echo "Redhat Linux detected."

        yum install -y gcc gcc-c++
        echo -e "g++ installed \n"

        yum install -y make
        echo -e "make installed \n"

        yum install -y readline readline-devel
        echo -e "readline-devel installed \n"

        yum install -y libcurl-devel
        echo -e "libcurl-devel installed \n"

        yum install -y libarchive
        yum install -y cmake
        echo "cmake installed"

        # boost 1.67.0
        mkdir boost
        cd boost
        wget https://sourceforge.net/projects/boost/files/boost/1.67.0/boost_1_67_0.tar.bz2
        tar -xvf boost_1_67_0.tar.bz2 && cd boost_1_67_0
        #./bootstrap.sh --prefix=/home/usrname/boost_1_43_0/boost_install
        # by default: /usr/local/include and /usr/local/lib
        ./bootstrap.sh
        ./b2
        ./b2 install
        echo "/usr/local/lib" >> /etc/ld.so.conf
        cd ../..
        rm -rf boost
        echo -e "boost-devel 1.67 installed \n"
    elif [ -f /etc/debian_version ]; then
        echo "Ubuntu/Debian Linux detected."

        apt install -y aptitude
        echo -e "aptitude installed to search softwares conveniently \n"

        apt install -y gcc g++
        echo -e "g++ installed \n"

        apt install -y make
        echo -e "make installed \n"

        apt install -y libboost-all-dev
        echo -e "libboost-all-dev installed \n"

        apt install -y libreadline-dev
        echo -e "readline-devel installed \n"

        apt install -y curl libcurl4 libcurl4-openssl-dev
        echo -e "libcurl-devel installed \n"

        apt install -y cmake
        echo -e "cmake installed \n"
    else
        echo "Sorry. Your Linux distribution can't run this script."
    fi
fi
