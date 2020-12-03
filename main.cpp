// parsing
#include <iostream>
#include <fstream>
#include <vector>
#include <sstream>
#include "Date/date.h"
#include <thread>
using namespace date;


// condition variables
// ring buffer or a SPSC queue


// timepoint based on internal highres with duration component that is the milliseconds preset
std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> get_current_timepoint(){
    return std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());
}

std::string chunk_to_message(std::vector<std::string> &chunk){
    std::string s;
    for (const auto &piece : chunk) s += piece;
    return s;
}


void parse_line_to_individual_strings(std::string &line, std::vector<std::string> &splittedStringArray){
    splittedStringArray.clear();
    size_t last = 0, pos = 0;
    while ((pos = line.find(',', last)) != std::string::npos) {
        splittedStringArray.emplace_back(line, last, pos - last);
        last = pos + 1;
    }
    return;
}



struct guardedvector {
    std::mutex guard;
    std::vector<std::string> myvector;
};

// ideal architecture:
// non timed threaded loop reads in lines and dumps chunks into array of limited size
// timed threaded sender sends via tcp
// if array limit (max total chunks) reached, pause readline. Continue to empty array via sending

struct Buffer{

};


void readlines_2(std::string filename, int timestamp_id, int max_chunk_size, Buffer buffer, std::condition_variable event_alive, std::condition_variable event_buffer_data_empty, std::condition_variable event_buffer_data_full){

}

void sendlines(Buffer buffer, std::condition_variable event_buffer_data_empty, std::condition_variable event_buffer_data_full, float time_scale=1.0){

}






// void readlines(std::fstream &infile, int max_chunk, guardedvector &v, int timestamp_id,  int yearstamp_id){
// void readlines(std::string filename, int max_chunk, std::vector<std::string> &v, int timestamp_id,  int yearstamp_id){
void readlines(std::string filename, int max_chunk, guardedvector &v, int timestamp_id,  int yearstamp_id){
    // init reader
    std::fstream infile(filename);
    char buffer[65536];
    infile.rdbuf()->pubsetbuf(buffer, sizeof(buffer));
    // current line
    std::string line;
    // splitted string of the current raw line
    std::vector<std::string> splittedString;

    bool reading = true;
    // string timestamp of next packet
    std::string next_packet_timestamp_raw;
    // chunk of strings to be concatenated
    std::vector<std::string> chunk(max_chunk);
    // duration, duration since we registered the last timestep in the file
    std::chrono::milliseconds time_passed_since_sending_last_timestep(0);
    // duration of time passed since last send
    std::chrono::milliseconds delta_dat(0);
    // duration of how long we actually wait, if at all
    std::chrono::milliseconds wait_for(0);
    // timepoint at which we registered the last timstep
    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> time_at_last_timestep_tp;
    // timepiont - absolute time of current line
    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> current_packet_timestamp_tp;
    // timepiont - absolute time of next line
    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> next_packet_timestamp_tp;

    // pre-loop
    getline(infile, line);
    std::cout << "header: " << line <<::std::endl;
    getline(infile, line);
    parse_line_to_individual_strings(line, splittedString);


    // parse string to tp
    std::string current_packet_timestamp_raw;
    current_packet_timestamp_raw = splittedString[yearstamp_id] + " " + splittedString[timestamp_id]; // convert this to a timepoint!
    std::stringstream ss_raw(current_packet_timestamp_raw);
    ss_raw >> parse("%Y%m%d %H:%M:%S", current_packet_timestamp_tp);
    std::cout << "First timestamp: " << current_packet_timestamp_tp <<::std::endl;

    // last prep
    time_at_last_timestep_tp = get_current_timepoint();


    while(reading){
        chunk.push_back(line);
        while (getline(infile, line)) {
            if(infile.eof()){
                reading = false;
                break;
            }
            // time passed between last send and start of this read - this is a duration since it is now only relative and has no absolute time relation
            time_passed_since_sending_last_timestep = std::chrono::milliseconds{get_current_timepoint() - time_at_last_timestep_tp};

            // check if we are ok with time (this can violate, let it violate for now)
            if(time_passed_since_sending_last_timestep > delta_dat){
                std::cout<<"timing violated"<<std::endl;
            }

            // parse current line
            parse_line_to_individual_strings(line, splittedString);
            next_packet_timestamp_raw = splittedString[yearstamp_id] + " " + splittedString[timestamp_id];
            std::stringstream ss_loop(next_packet_timestamp_raw);
            ss_loop >> parse("%Y%m%d %H:%M:%S", next_packet_timestamp_tp);
            std::cout << "next packet timestamp: " << next_packet_timestamp_tp <<::std::endl;

            // check if we are still in same time point and if the chunk is not filled
            if(next_packet_timestamp_tp==current_packet_timestamp_tp && chunk.size()<max_chunk){
                chunk.push_back(line);
                std::cout<<"accumulate"<<std::endl;
            }else if((next_packet_timestamp_tp==current_packet_timestamp_tp) && (chunk.size()>=max_chunk)){
                std::cout<<"max chunk size reached, sending"<<std::endl;
                // parse to string
                std::string m = chunk_to_message(chunk);

                // send to buffer
                v.guard.lock();
                v.myvector.push_back(m);
                v.guard.unlock();
//                v.push_back(m);

                // register time
                time_at_last_timestep_tp = get_current_timepoint();
                chunk.clear();
            }else if((next_packet_timestamp_tp!=current_packet_timestamp_tp) && (chunk.size()<max_chunk)){
                std::cout<<"reached new timestep"<< next_packet_timestamp_tp <<", chunk size not maxed out, sending"<<std::endl;
                // purge data
                // parse to string
                std::string m = chunk_to_message(chunk);

                // send to buffer
                v.guard.lock();
                v.myvector.push_back(m);
                v.guard.unlock();
//                v.push_back(m);

                // register time
                time_at_last_timestep_tp = get_current_timepoint();
                chunk.clear();
                break;
            }else if((next_packet_timestamp_tp!=current_packet_timestamp_tp) && (chunk.size()>=max_chunk)){
                std::cout<<"reached new timestep"<< next_packet_timestamp_tp <<", chunk size maxed out, sending"<<std::endl;
                // purge data
                // parse to string
                std::string m = chunk_to_message(chunk);

                // send to buffer
                v.guard.lock();
                v.myvector.push_back(m);
                v.guard.unlock();
//                v.push_back(m);

                // register time
                time_at_last_timestep_tp = get_current_timepoint();
                chunk.clear();
                break;
            }else{
                std::cout<<"something else"<<std::endl;
                break;
            }
        }
        if(reading){
            // make sure we have the most truthful representation of the last timepint we sent something
            time_passed_since_sending_last_timestep = std::chrono::milliseconds{get_current_timepoint() - time_at_last_timestep_tp};
            // wait in case of realtime. Here, we can also introduce amortization. (remove wait times to catch up)
            if(time_passed_since_sending_last_timestep < delta_dat){
                wait_for = delta_dat - time_passed_since_sending_last_timestep;
                std::cout<<"waiting until remaining time passed to advance to next timestep "<< wait_for <<std::endl;
                std::this_thread::sleep_for(wait_for);
            }

            // we've reached the next timestep
            // we calculate how long we have time now to send the current timestep
            delta_dat = next_packet_timestamp_tp - current_packet_timestamp_tp;
            std::cout<<"new delta is "<< delta_dat<< std::endl;

            // advance
            current_packet_timestamp_tp = next_packet_timestamp_tp;
        }
    }
    return;
}




int main() {

    int timestamp_id = 1;
    int yearstamp_id = 0;
    int max_chunk = 100;
    float time_scale = 1.0;
    std::string UDP_IP = "127.0.0.1";
    int UDP_PORT = 5005;
    std::string csv_path = "sample.csv";
    bool wait = false;

    // init reader
    std::string filename = "/Users/estebanlanter/PycharmProjects/LimitOrderBook/equity_full_depth_0930_1600.csv";


    /* Write some stuff and read the echoes. */
    puts("Connect to server, about to write some stuff...");

//    std::vector<std::string> v;
//    std::thread test(readlines, filename, max_chunk, std::ref(v), timestamp_id,  yearstamp_id);
//    test.join();

    guardedvector v;
    std::thread test(readlines, filename, max_chunk, std::ref(v), timestamp_id,  yearstamp_id);
    test.join();

}



// ideal:
// Thread 1: run thru, parse, put chunks into fifo queue.
// Thread 2: Process fifo queue with sending according to timestamp. If sending cant keep up, the earliest elements in the fifo will be pushed out of the way.