//
//  main.cpp
//  conducteur
//
//  Created by Jean-Luc on 4/5/18.
//  Copyright © 2018 Security Sentinels LLC. All rights reserved.
//

#include <iostream>
#include <pqxx/pqxx>
#include <thread>
#include <chrono>
#include <unistd.h>
#include <string>
#include <errno.h>



int main(int argc, const char * argv[]) {
    
    const int RECORDS_PER_BATCH = 300  ;
    const std::string CONN_STRING = "user=instigator_app  host=jeanluc-db.c0hxc3wgxzra.us-east-1.rds.amazonaws.com  password=zfszsT38ED  dbname=instigator";
    bool engine_status = false;
    int pull_interval = 0;
    int stock_count = 0;
    char * PID = (char *)getpid;
    
    // Connect to DB and pull variables
    // Establish DB Connection
    std::string query_app_preferences ="SELECT engine_status, stock_count, pull_interval FROM app_preferences";
    pqxx::connection C{CONN_STRING};
    pqxx::work W{C};
    pqxx::result R = W.exec(query_app_preferences);
    W.commit();
    
    // Let's SET engine_status, pull_interval, stock_count from db
    engine_status = R[0]["engine_status"].as<bool>();
    pull_interval = R[0]["pull_interval"].as<int>();
    stock_count = R[0]["stock_count"].as<int>();
    std::cout << "CONDUCTEUR::Engine Status:" << engine_status << std::endl;
    std::cout << "CONDUCTEUR::Pull Interval:" << pull_interval << std::endl;
    std::cout << "CONDUCTEUR::Stock Count:" << stock_count << std::endl;
    
    
    // Let's figure out how many looper to spawn
    // make sure we have stocks to pull
    if (stock_count > 0) {
        int scooper_spawn_count = (stock_count / RECORDS_PER_BATCH) + 1;
        std::cout << "CONDUCTEUR::There is " << stock_count << " stock names to pull. " << std::endl;
        std::cout << "CONDUCTEUR::I'm going to spawn " << scooper_spawn_count << " scoopers to handle the load." << std::endl;
        
        // spawn the scoopers
        // Create argument array with PID for system call
        char *args[] = {(char*)"/usr/local/bin/scooper", (char *)PID , (char *) 0 };
//          std::string e = "/usr/local/bin/scooper " + std::to_string(PID);
//          const char *  scooper_call = e.c_str();
            // Lets loop through scooper_spawn_count
            for (int i=1; i <= scooper_spawn_count; i++) {
                pid_t child_pID = fork();
                if (child_pID == 0)                // child
                {
                    // Function call does not return on success.
                    errno = 0;
                    int execReturn = execv("/usr/local/bin/scooper", args);
                    if(execReturn == -1)  {
                        std::cout << "CONDUCTEUR::Failure! execv error code=" << errno << std::endl;
                    }
                    if(execReturn == -1) {
                        std::cout << "CONDUCTEUR::Failure! execv error code=" << errno << std::endl;
                    }
                    _exit(0); // If exec fails then exit forked process.
                }
                else if (child_pID < 0)  {           // failed to fork
                    std::cerr << "CONDUCTEUR::Failed to fork" << std::endl;
                }else{                         // parent
                    std::cout << "CONDUCTEUR::Parent Process" << std::endl;
                }
                std::cout << "CONDUCTEUR::scooper " << i << " launched " << args[1] << std::endl;
            }
        
        // Let's start the timer
        auto start_scooping = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());
        
        // Let's start populating records in the queue for the looper slaves
        std::string scoop_stocks ="UPDATE stock_symbols set scoop = true";
        pqxx::connection C{CONN_STRING};
        pqxx::work W{C};
        W.exec(scoop_stocks);
        W.commit();
        
        
        // Let's wait until engine turns on
        while(!engine_status){
            std::this_thread::sleep_for(std::chrono::milliseconds(30000));
            std::cout << "CONDUCTEUR::Sleeping 30 Seconds  " << std::endl;
        };
        
        
        // Let's continue until engine turn off
        while(engine_status){
            // Let's figure out how much time to wait until we pull
            // Let's get a time stamp and get milliseconds since epoch
            auto end_scooping  = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());
            auto start_since_epoc  = start_scooping.time_since_epoch();
            auto end_since_epoc = end_scooping.time_since_epoch();
            auto start_epoc_ms = std::chrono::duration_cast<std::chrono::milliseconds>(start_since_epoc);
            auto end_epoc_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_since_epoc);
            
            // let's get how many milliseconds since the program started
            long duration_ms = end_epoc_ms.count() - start_epoc_ms.count();
            std::cout << "CONDUCTEUR::Processing Time " << duration_ms << "ms" << std::endl;
            
            // Let's get the thread to sleep for the remaining 5 minutes
            // auto sleepy_time = 300000 - duration_ms; // 300 seconds or 5 minutes
            auto sleepy_time = (pull_interval * 1000) - duration_ms; // 300 seconds or 5 minutes
            std::cout << "CONDUCTEUR::Sleeping for " << sleepy_time << "ms" << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepy_time));
            
            // Let's enable records in the queue for the looper slaves
//            pqxx::connection C{CONN_STRING};
//            pqxx::work W{C}; 
            W.exec(scoop_stocks);
            W.commit();
    
            // Let's check if the engine is still on
            // Connect to DB and pull variables
            // Establish DB Connection
            pqxx::result R = W.exec(query_app_preferences);
            W.commit();

            // Let's GET engine_status
            engine_status = R[0]["engine_status"].as<bool>();
            std::cout << "CONDUCTEUR::Engine Status:" << engine_status << std::endl;
        };
        
    }else{
        std::cout << "CONDUCTEUR::There is nothing to do here.." << std::endl;
        std::cout << "CONDUCTEUR::Exiting.." << std::endl;
    }
    
    return 0;
}
