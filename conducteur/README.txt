# README FILE
# April 5th
# Jean-Luc Langlois
# miamisp@mac.com


# Necessary libraries:
sudo apt install build-essential
sudo apt install curl
sudo apt install libcurl4-openssl-dev
sudo apt install openssl
sudo apt install git
sudo apt install libpqxx-dev
sudo apt install libcurlpp-dev
sudo apt install cmake

# Fixing libcurlpp:
sudo apt-get remove libcurlpp0
cd [wherever]
git clone https://github.com/jpbarrette/curlpp.git
cd curlpp
cmake .
sudo make install
cd [back to deepdetect]
make clean
make

# Checking xml2 flags
use  in g++
`xml2-config --cflags` and `xml2-config --libs`
or the following at the command prompt
xml2-config --cflags
xml2-config --libs

# g++ command
g++ -L/usr/include -lpqxx -c main.cpp -o conducteur
