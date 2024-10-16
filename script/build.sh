# antlr
cd queryDecompose/tools
tar -zxvf libantlr3c-3.4.tar.gz
cd libantlr3c-3.4/src
gcc -c -O -I.. -I../include *.c
cd ../../../
rm -f lib/libantlr.a
ar -crv lib/libantlr.a tools/libantlr3c-3.4/src/*.o
cd ..

mkdir build ; cd build
cmake ..
make -j8