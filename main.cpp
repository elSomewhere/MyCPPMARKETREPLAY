// parsing
#include <iostream>
#include <fstream>
#include <vector>
#include <sstream>
#include "Date/date.h"
#include <thread>
using namespace date;


// ideal architecture:
// non timed threaded loop reads in lines and dumps chunks into array of limited size
// timed threaded sender sends via tcp
// if array limit (max total chunks) reached, pause readline. Continue to empty array via sending

// condition variables
// ring buffer or a SPSC queue


// timepoint based on internal highres with duration component that is the milliseconds preset

#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>

#include <stdexcept>
#include <iostream>
#include <list>
#include <vector>
#include <queue>







template<typename T>
class queue {
    std::deque<T> content;
    size_t capacity;

    std::mutex mutex;
    std::condition_variable not_empty;
    std::condition_variable not_full;

    // i tihnk this to avoid that queue is called with these
    queue(const queue &) = delete;
    queue(queue &&) = delete;
    queue &operator = (const queue &) = delete;
    queue &operator = (queue &&) = delete;

public:
    queue(size_t capacity): capacity(capacity) {}

    void push(T &&item) {
        {
            std::unique_lock<std::mutex> lk(mutex);
            not_full.wait(lk, [this]() { return content.size() < capacity; });
            content.push_back(std::move(item));
        }
        not_empty.notify_one();
    }

    bool try_push(T &&item) {
        {
            std::unique_lock<std::mutex> lk(mutex);
            if (content.size() == capacity)
                return false;
            content.push_back(std::move(item));
        }
        not_empty.notify_one();
        return true;
    }

    void pop(T &item) {
        {
            std::unique_lock<std::mutex> lk(mutex);
            not_empty.wait(lk, [this]() { return !content.empty(); });
            item = std::move(content.front());
            content.pop_front();
        }
        not_full.notify_one();
    }

    bool try_pop(T &item) {
        {
            std::unique_lock<std::mutex> lk(mutex);
            if (content.empty())
                return false;
            item = std::move(content.front());
            content.pop_front();
        }
        not_full.notify_one();
        return true;
    }
};







// intermediate type for the loop
struct timed_chunk{
    std::vector<std::string> chunk;
    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> last_timestamp;
};


// the element that is held in the queue
struct Message{
    std::string chunk_bytes;
    std::chrono::milliseconds timer;
};



//// make a template instead of holding messages with std::String
class Buffer
{
public:
    Buffer(size_t capacity, std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> init_timestamp): buf(capacity), tail_time(init_timestamp){}
    void push(std::string const & v, std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> last_timestamp){
        std::chrono::milliseconds delta_t = last_timestamp - tail_time;
        std::cout<<"pushed a delta_t message of "<<delta_t<<::std::endl;
        Message message{v, delta_t};
        buf.push(std::move(message));
        tail_time = last_timestamp;
    }
    void pop(Message &m){
        buf.pop(m);
    }

private:
    queue<Message> buf;
    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> tail_time;
};



//// make a template instead of holding messages with std::String
//class Buffer
//{
//public:
//    void push(std::string const & v, std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> last_timestamp){
//        std::chrono::milliseconds delta_t = last_timestamp - tail_time;
//        Message message{v, delta_t};
//        buf.push(message);
//        tail_time = last_timestamp;
//    }
//    Message pop(Message &m){
//        buf.pop();
//    }
//
//private:
//    std::queue<Message> buf;
//    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> tail_time;
//};



std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> get_current_timepoint(){
    return std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());
}


// parse vector of lines to one big line and transform it to raw bytes (bytes type should rather be template type)
std::string chunk_to_rawbytes(std::vector<std::string> &chunk){
    std::string s;
    for (const auto &piece : chunk) s += piece;
    return s;
}


// OK
void parse_line_to_individual_strings(std::string &line, std::vector<std::string> &splittedStringArray){
    splittedStringArray.clear();
    size_t last = 0, pos = 0;
    while ((pos = line.find(',', last)) != std::string::npos) {
        splittedStringArray.emplace_back(line, last, pos - last);
        last = pos + 1;
    }
    return;
}


// OK
std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> get_timestamp_from_line(std::string &line, std::vector<std::string> &splittedString, int yearstamp_id, int timestamp_id){
    // parse string to tp
    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> res;
    parse_line_to_individual_strings(line, splittedString);
    std::string current_packet_timestamp_raw;
    current_packet_timestamp_raw = splittedString[yearstamp_id] + " " + splittedString[timestamp_id]; // convert this to a timepoint!
    std::stringstream ss_raw(current_packet_timestamp_raw);
    ss_raw >> parse("%Y%m%d %H:%M:%S", res);
    return res;
}


std::string parselinefun(std::string line){
    return line;
}


timed_chunk parse_raw_line(std::string &line, std::vector<std::string> &splittedString, std::vector<std::string> &chunk, int max_chunk_size, int yearstamp_id, int timestamp_id, std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> &last_timestamp, Buffer &buffer){
    std::chrono::milliseconds timestamp_delta(0);
    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> current_timestep = get_timestamp_from_line(line, splittedString, yearstamp_id, timestamp_id);
    timestamp_delta = std::chrono::milliseconds{current_timestep - last_timestamp};
    std::cout<<"current_timestep is "<<current_timestep<<std::endl;
    std::cout<<"delta to last timestep is "<<timestamp_delta<<std::endl;
    if(timestamp_delta.count()==0 & chunk.size()<max_chunk_size){
        std::string parsedline = parselinefun(line);
        chunk.push_back(parsedline);
        timed_chunk res{
                chunk,
                last_timestamp
        };
        return res;
    }else if(timestamp_delta.count()==0 & chunk.size()>=max_chunk_size){
        std::string message = chunk_to_rawbytes(chunk);
        buffer.push(message, last_timestamp);
        chunk.clear();
        std::string parsedline = parselinefun(line);
        chunk.push_back(parsedline);
        timed_chunk res{
                chunk,
                last_timestamp
        };
        return res;
    }else if(timestamp_delta.count()>0 & chunk.size()<max_chunk_size){
        std::string message = chunk_to_rawbytes(chunk);
        buffer.push(message, last_timestamp);
        last_timestamp = get_timestamp_from_line(line, splittedString, yearstamp_id, timestamp_id);
        chunk.clear();
        std::string parsedline = parselinefun(line);
        chunk.push_back(parsedline);
        timed_chunk res{
                chunk,
                last_timestamp
        };
        return res;
    }else if(timestamp_delta.count()>0 & chunk.size()>=max_chunk_size){
        std::string message = chunk_to_rawbytes(chunk);
        buffer.push(message, last_timestamp);
        last_timestamp = get_timestamp_from_line(line, splittedString, yearstamp_id, timestamp_id);
        chunk.clear();
        std::string parsedline = parselinefun(line);
        chunk.push_back(parsedline);
        timed_chunk res{
                chunk,
                last_timestamp
        };
        return res;
    }
}


void readlines(std::string filename, int timestamp_id, int yearstamp_id, int max_chunk_size, Buffer &buff){
    std::fstream infile(filename);
    char buffer[65536];
    infile.rdbuf()->pubsetbuf(buffer, sizeof(buffer));
    // current line
    std::string line;
    // splitted line (for date parsing)
    std::vector<std::string> splittedString;
    // resulting timepoint
    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> last_timestamp;
    // dispose of header
    getline(infile, line);
    // the container for individual chunks
    std::vector<std::string> chunk(max_chunk_size);
    // get first line
    getline(infile, line);
    // parse to timesdatpm
    last_timestamp = get_timestamp_from_line(line, splittedString, yearstamp_id, timestamp_id);
    // probably now have to fil the first element into the chunk
    timed_chunk temp = parse_raw_line(line, splittedString, chunk, max_chunk_size, yearstamp_id, timestamp_id, last_timestamp, buff);
    bool reading = true;
    while(reading){
        while (getline(infile, line)){
            if(infile.eof()){
                reading = false;
                break;
            }
            temp = parse_raw_line(line, splittedString, chunk, max_chunk_size, yearstamp_id, timestamp_id, last_timestamp, buff);
            getline(infile, line);
        }
    }
}


void sendlines(Buffer &buff, float time_scale=1.0){
//    //TODO
    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> time_at_last_sent_message = get_current_timepoint();
    std::chrono::milliseconds next_message_delta;
    std::chrono::milliseconds time_passed_since_last_send_op;
    std::chrono::milliseconds wait_for;
    std::chrono::milliseconds zero_duration(0);
    while(true){
        std::cout<<"sendlines"<<std::endl;
        Message message;
        buff.pop(message);
        next_message_delta = message.timer;
        std::cout<<"next_message_delta is "<<next_message_delta<<std::endl;
        std::string rawmessage = message.chunk_bytes;
        time_passed_since_last_send_op = get_current_timepoint()-time_at_last_sent_message;
        wait_for = std::max(next_message_delta - time_passed_since_last_send_op, zero_duration);
        std::cout<<"wait for "<<wait_for<<std::endl;
        std::this_thread::sleep_for(wait_for);
        std::cout<<"sending"<<std::endl;
        time_at_last_sent_message = get_current_timepoint();
    };
}




std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> peek_first_datepoint_from_file(std::string filename, int yearstamp_id, int timestamp_id){
    // peak to get inital time
    std::fstream infile(filename);
    char buffer[65536];
    infile.rdbuf()->pubsetbuf(buffer, sizeof(buffer));
    // current line
    std::string line;
    // splitted line (for date parsing)
    std::vector<std::string> splittedString;
    // resulting timepoint
    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> last_timestamp;
    // dispose of header
    getline(infile, line);
    // get first line
    getline(infile, line);
    // parse to timesdatpm
    last_timestamp = get_timestamp_from_line(line, splittedString, yearstamp_id, timestamp_id);
    return last_timestamp;
}




int main() {

    int timestamp_id = 1;
    int yearstamp_id = 0;
    int max_chunk_size = 1000;
    int max_buff_size = 1000;
    float time_scale = 1.0;
    std::string filename = "/Users/estebanlanter/PycharmProjects/LimitOrderBook/equity_full_depth_0930_1600.csv";
    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> last_timestamp = peek_first_datepoint_from_file(filename, yearstamp_id, timestamp_id);
    // the buffer communicating between reader and sender
    Buffer buff(max_buff_size, last_timestamp);
    //start threads
    std::thread test1(readlines, filename, timestamp_id, yearstamp_id,  max_chunk_size, std::ref(buff));
    std::thread test2(sendlines, std::ref(buff), 1.0);
    //readlines(filename, timestamp_id, yearstamp_id, max_chunk_size, buff);
    //sendlines(buff, 1.0);
    test1.join();
    test2.join();
    return 0;

}
