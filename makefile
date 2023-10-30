CC = g++
CFLAGS = -pthread -O3 -g -fno-omit-frame-pointer 
# -Wall

TARGET = run_db

SRCS = db_main.cc

OBJS = $(SRCS:.cpp=.o)

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^

run: $(TARGET)
	./$(TARGET)

clean:
	rm -f $(TARGET)
