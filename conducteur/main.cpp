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


// Function to enable db scoop stocks
bool scoop_stocks (){
    
    // Let's start populating records in the queue for the scooper slaves
    const std::string CONN_STRING = "user=instigator_app  host=jeanluc-db.c0hxc3wgxzra.us-east-1.rds.amazonaws.com  password=zfszsT38ED  dbname=instigator";
    std::string scoop_stocks ="UPDATE stock_symbols set scoop = true";
    pqxx::connection C{CONN_STRING};
    pqxx::work W{C};
    try {
        W.exec(scoop_stocks);
        W.commit();
    } catch (const std::exception& e) {
        W.abort();
        std::cerr << e.what();
        return false;
    }
    return true;
}

// Function to get App Preferences
bool get_app_preferences(bool &engine_status, int &pull_interval, int &stock_count){
    
    const std::string CONN_STRING = "user=instigator_app  host=jeanluc-db.c0hxc3wgxzra.us-east-1.rds.amazonaws.com  password=zfszsT38ED  dbname=instigator";
    std::string query_app_preferences ="SELECT engine_status, stock_count, pull_interval FROM app_preferences";
    pqxx::connection C{CONN_STRING};
    pqxx::work W{C};
    try {
        pqxx::result R = W.exec(query_app_preferences);
        W.commit();
    
        // Let's SET engine_status, pull_interval, stock_count from db
        engine_status = R[0]["engine_status"].as<bool>();
        pull_interval = R[0]["pull_interval"].as<int>();
        stock_count = R[0]["stock_count"].as<int>();
        std::cout << "CONDUCTEUR::Pulling App Preferences:" << std::endl;
        std::cout << "CONDUCTEUR::Engine Status:" << engine_status << std::endl;
        std::cout << "CONDUCTEUR::Pull Interval:" << pull_interval << std::endl;
        std::cout << "CONDUCTEUR::Stock Count:" << stock_count << std::endl;
    } catch (const std::exception& e) {
        W.abort();
        std::cerr << e.what();
        return false;
    }
    return true;
}

int main(int argc, const char * argv[]) {
    
    // Set constants and variables
    // const int RECORDS_PER_BATCH = 300  ;
    const std::string CONN_STRING = "user=instigator_app  host=jeanluc-db.c0hxc3wgxzra.us-east-1.rds.amazonaws.com  password=zfszsT38ED  dbname=instigator";
    bool engine_status = false;
    int pull_interval = 0;
    int stock_count = 0;
    
    // Connect to DB and pull app preferences
    if(get_app_preferences(engine_status, pull_interval, stock_count)){
        std::cout << "CONDUCTEUR::App Preferences Pulled!!" << std::endl;
    }else{
        std::cout << "CONDUCTEUR::ERROR::Unable to pull App Preferences!!" << std::endl;
    }
    
    // Let's start the timer
    //auto start_scooping = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());

    // Let's wait until engine turns on
    while(!engine_status){
         std::this_thread::sleep_for(std::chrono::seconds(10));
         std::cout << "CONDUCTEUR::Engine OFF! Sleeping 10 Seconds  " << std::endl;
        get_app_preferences(engine_status, pull_interval, stock_count);
    };
        
    // Let's continue until engine turn off
    while(engine_status && stock_count > 0){
        // Let's get a time stamp and get milliseconds since epoch
        auto start_time  = std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
        auto start_time_since_epoc  = start_time.time_since_epoch();
        
        // Let's enable records in the queue for the looper slaves
        // Let's start populating records in the queue for the looper slaves
        if(scoop_stocks()){
            std::cout << "CONDUCTEUR::Scooped stocks " << std::endl;
        }else{
            std::cout << "CONDUCTEUR::ERROR::Unable to scoop stocks " << std::endl;
        }
        
        // Let's figure out how much time to wait until we pull
        auto end_time  = std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
        auto end_time_since_epoc = end_time.time_since_epoch();
        
//        auto start_epoc = std::chrono::duration_cast<std::chrono::seconds>(start_time_since_epoc);
//        auto end_epoc = std::chrono::duration_cast<std::chrono::seconds>(end_since_epoc);
        
        // let's get how many seconds since the program started
        long duration = end_time_since_epoc.count() - start_time_since_epoc.count();
        std::cout << "CONDUCTEUR::Processing Time " << duration << " seconds" << std::endl;
        
        // Let's get the thread to sleep for the remaining 5 minutes
        // auto sleepy_time = 300000 - duration_ms; // 300 seconds or 5 minutes
        auto sleepy_time = pull_interval - duration; // 300 seconds or 5 minutes
        std::cout << "CONDUCTEUR::Sleeping for " << sleepy_time << " seconds" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(sleepy_time));
        
        // Let's check if the engine is still on
        // Connect to DB and pull app preferences
        if(get_app_preferences(engine_status, pull_interval, stock_count)){
            std::cout << "CONDUCTEUR::App Preferences Pulled " << std::endl;
        }else{
            std::cout << "CONDUCTEUR::ERROR::Unable to pull App Preferences " << std::endl;
        }
        
        std::cout << std::endl;
        
    };
    
    return 0;
}
