all:
	g++ -std=c++17 src/main.cc -lpthread -lhiredis -lz -o redis-migrate
clean:
	rm redis-migrate