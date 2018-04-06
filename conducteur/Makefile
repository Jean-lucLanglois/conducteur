
# define variables
CC:g++
CFLAGS:-std=c++11 -c -Wall
CPPFLAGS:-I/usr/include/  -lcurl -lcurlpp -lpqxx


# build
all: conducteur

conducteur: main.o
$(CC) main.o conducteur

main.o: main.cpp
$(CC) $(CFLAGS) main.cpp

clean:
rm -rf *.o conducteur
