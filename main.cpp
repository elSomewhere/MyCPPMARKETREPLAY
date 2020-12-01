// socketing
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <unistd.h>

// parsing
#include <iostream>
#include <fstream>
#include <vector>
#include <sstream>
#include "Date/date.h"
#include <thread>
using namespace date;

// globals
#define PortNumber 1200
#define MaxConnects 8
#define BuffSize 256
#define ConversationLen    3
#define Host "localhost"








void report(const char* msg, int terminate) {
    perror(msg);
    if (terminate) exit(-1); /* failure */
}


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


bool send_over_tcp(std::string m, int socket_file_descriptor){
    bool res = write(socket_file_descriptor, &m[0], strlen(&m[0])) > 0;
    return res;
};

void send_and_receive_echo_over_tcp(std::string m, int socket_file_descriptor){
    bool arrived = send_over_tcp(m, socket_file_descriptor);
    if(arrived){
        char buffer[BuffSize + 1];
        memset(buffer, '\0', sizeof(buffer));
        if (read(socket_file_descriptor, buffer, sizeof(buffer)) > 0)
            puts(buffer);
    }
};

struct guardedvector {
    std::mutex guard;
    std::vector<std::string> myvector;
};




int main() {


    /* file descriptor for the socket */
    int socket_file_descriptor = socket(
            AF_INET,      /* versus AF_LOCAL */
            SOCK_STREAM,  /* reliable, bidirectional */
            0);           /* system picks protocol (TCP) */
    if (socket_file_descriptor < 0) report("socket", 1); /* terminate */

    /* get the address of the host */
    struct hostent* hptr = gethostbyname(Host); /* localhost: 127.0.0.1 */
    if (!hptr) report("gethostbyname", 1); /* is hptr NULL? */
    if (hptr->h_addrtype != AF_INET)       /* versus AF_LOCAL */
        report("bad address family", 1);

    /* connect to the server: configure server's address 1st */
    struct sockaddr_in saddr;
    memset(&saddr, 0, sizeof(saddr));
    saddr.sin_family = AF_INET;
    saddr.sin_addr.s_addr =
            ((struct in_addr*) hptr->h_addr_list[0])->s_addr;
    saddr.sin_port = htons(PortNumber); /* port number in big-endian */

    if (connect(socket_file_descriptor, (struct sockaddr*) &saddr, sizeof(saddr)) < 0)
        report("connect", 1);


    //////////////////////////


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
    std::chrono::milliseconds time_passed_since_sending_last_timestep(0);
    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> current_packet_timestamp_tp;
    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> next_packet_timestamp_tp;
    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> time_at_last_timestep_tp;

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
    time_at_last_timestep_tp = get_current_timepoint();



    /* Write some stuff and read the echoes. */
    puts("Connect to server, about to write some stuff...");

    bool reading = true;
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

            // measure passed time duration the last timepoint in the data

            // check if we are still in same time point and if the chunk is not filled
            if(next_packet_timestamp_tp==current_packet_timestamp_tp && chunk.size()<max_chunk){
                chunk.push_back(line);
                std::cout<<"accumulate"<<std::endl;
            }else if((next_packet_timestamp_tp==current_packet_timestamp_tp) && (chunk.size()>=max_chunk)){
                std::cout<<"max chunk size reached, sending"<<std::endl;
                // parse to string
                std::string m = chunk_to_message(chunk);
                // send over tcp
                //send_over_tcp(m, socket_file_descriptor);
                // register time
                time_at_last_timestep_tp = get_current_timepoint();
                chunk.clear();
            }else if((next_packet_timestamp_tp!=current_packet_timestamp_tp) && (chunk.size()<max_chunk)){
                std::cout<<"reached new timestep"<< next_packet_timestamp_tp <<", chunk size not maxed out, sending"<<std::endl;
                // purge data
                // parse to string
                std::string m = chunk_to_message(chunk);
                // send over tcp
                //send_over_tcp(m, socket_file_descriptor);
                // register time
                time_at_last_timestep_tp = get_current_timepoint();
                chunk.clear();
                break;
            }else if((next_packet_timestamp_tp!=current_packet_timestamp_tp) && (chunk.size()>=max_chunk)){
                std::cout<<"reached new timestep"<< next_packet_timestamp_tp <<", chunk size maxed out, sending"<<std::endl;
                // purge data
                // parse to string
                std::string m = chunk_to_message(chunk);
                // send over tcp
                //send_over_tcp(m, socket_file_descriptor);
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

}



// ideal:
// Thread 1: run thru, parse, put chunks into fifo queue.
// Thread 2: Process fifo queue with sending according to timestamp. If sending cant keep up, the earliest elements in the fifo will be pushed out of the way.