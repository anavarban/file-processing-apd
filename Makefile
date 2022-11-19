build:
	gcc tema1.c -o tema1 -lpthread -Wall -lm

build_debug:
	gcc tema1.c -o tema1 -lpthread -Wall -lm -DDEBUG -g3 -O0 -Wall

clean:
	rm tema1 out*.txt
