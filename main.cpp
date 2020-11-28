#include <iostream>
#include <fstream>
#include <vector>
#include <sstream>
#include "Date/date.h"
#include <thread>
using namespace date;
// read csv in realtime



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
    std::fstream infile(filename);
    char buffer[65536];
    infile.rdbuf()->pubsetbuf(buffer, sizeof(buffer));
    std::string line;
    std::vector<std::string> splittedString;


    // init sender
     std::vector<std::string> chunk(max_chunk);

    // init timing control
    std::string current_packet_timestamp_raw;
    std::string next_packet_timestamp_raw;
    std::chrono::milliseconds delta_dat(0);
    std::chrono::milliseconds wait_for(0);
    std::chrono::milliseconds time_since_last_send(0);
    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> current_packet_timestamp_tp;
    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> next_packet_timestamp_tp;
    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> time_at_last_send_tp;

    // pre-loop
    getline(infile, line);
    std::cout << "header: " << line <<::std::endl;
    getline(infile, line);
    std::cout << "First raw line: " << line <<::std::endl;
    parse_line_to_individual_strings(line, splittedString);
    current_packet_timestamp_raw = splittedString[yearstamp_id] + " " + splittedString[timestamp_id]; // convert this to a timepoint!


    // parse string to tp
    std::stringstream ss_raw(current_packet_timestamp_raw);
    ss_raw >> parse("%Y%m%d %H:%M:%S", current_packet_timestamp_tp);
    std::cout << "First timestamp: " << current_packet_timestamp_tp <<::std::endl;

    // send...
    time_at_last_send_tp = get_current_timepoint();



    bool reading = true;
    while(reading){
        chunk.push_back(line);
        while (getline(infile, line)) {
            if(infile.eof()){
                reading = false;
                break;
            }
            // time passed between last send and start of this read
            time_since_last_send = std::chrono::milliseconds{get_current_timepoint() - time_at_last_send_tp};

            // check if we are ok with time (this can violate, let it violate for now)
            if(time_since_last_send > delta_dat){
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
                chunk_to_message(chunk);
                time_at_last_send_tp = get_current_timepoint();
                chunk.clear();
            }else if((next_packet_timestamp_tp!=current_packet_timestamp_tp) && (chunk.size()<max_chunk)){
                std::cout<<"reached new timestep"<< next_packet_timestamp_tp <<", chunk size not maxed out, sending"<<std::endl;
                chunk_to_message(chunk);
                time_at_last_send_tp = get_current_timepoint();
                chunk.clear();
                break;
            }else if((next_packet_timestamp_tp!=current_packet_timestamp_tp) && (chunk.size()>=max_chunk)){
                std::cout<<"reached new timestep"<< next_packet_timestamp_tp <<", chunk size maxed out, sending"<<std::endl;
                chunk_to_message(chunk);
                time_at_last_send_tp = get_current_timepoint();
                chunk.clear();
                break;
            }else{
                std::cout<<"something else"<<std::endl;
                break;
            }
        }
        if(reading){
            // wait in case of realtime. Here, we can also introduce amortization. (remove wait times to catch up)
            if(time_since_last_send < delta_dat){
                wait_for = delta_dat - time_since_last_send;
                std::cout<<"waiting until remaining time passed to advance to next timestep "<< wait_for <<std::endl;
                std::this_thread::sleep_for(wait_for);
            }

            // send data
            // !!!!!!!!!!!!


            // we've reached the next timestep
            // we calculate how long we have time now to send the current timestep
            delta_dat = next_packet_timestamp_tp - current_packet_timestamp_tp;
            std::cout<<"new delta is "<< delta_dat<< std::endl;

            // advance
            current_packet_timestamp_tp = next_packet_timestamp_tp;
        }
    }

}
