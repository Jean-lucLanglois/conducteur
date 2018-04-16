CC:g++
CFLAGS:-std=c++11 -c -Wall
CPPFLAGS:-I/usr/include/  -lcurl -lcurlpp -lpqxx


# build
all: conducteur

conducteur: main.o
$(CC) ($CFLAGS) main.o -o conducteur ($CPPFLAGS)
    main.o: main.cpp

main.o: main.cpp
$(CC) $(CFLAGS) main.cpp ($CPPFLAGS)

clean:
rm -rf *.o conducteur
